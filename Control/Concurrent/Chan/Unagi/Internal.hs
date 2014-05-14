{-# LANGUAGE BangPatterns , DeriveDataTypeable #-}
module Control.Concurrent.Chan.Unagi.Internal (
     sEGMENT_LENGTH
    , InChan(..), OutChan(..), ChanEnd(..), StreamSegment, Cell(..), Stream(..), NextSegment(..)
    , newChanStarting, writeChan, readChan
    , throwKillTo, catchKillRethrow
    )
    where

-- Internals exposed for testing.

import Control.Concurrent.MVar
import Control.Concurrent(ThreadId)
import Data.IORef
-- import Control.Exception(mask_,assert,onException,throwIO,SomeException,Exception)
import Control.Exception
import qualified Control.Exception as E
import Data.Typeable

import Control.Monad.Primitive(RealWorld)
import Data.Atomics.Counter
import Data.Atomics
import qualified Data.Primitive as P

import Control.Monad
import Control.Applicative

import Data.Bits

-- TODO WRT GARBAGE COLLECTION
--  This can lead to large amounts of memory use in theory:
--   1. overhead of pre-allocated arrays and template
--   2. already-read elements in arrays not-yet GC'd
--   3. array and element overhead from a writer/reader delayed (many intermediate chunks)
-- Ideas:
--   - StablePtr can be freed manually. Does it allow array to be freed too?
--      but see: https://ghc.haskell.org/trac/ghc/ticket/7670
--   - does a single-constructor occurrence point to the same bit of memory?
--      (i.e. no element overhead for writing `Empty` or something?)
--   - hold weak reference to previous segment, and when we move the head
--     pointer, update `next` to point to this new segment (if it is still
--     around?)
-- TODO performance
--   - eliminate some indirection, by replacing IORefs with Arrays of Arrays
--   - replace constructors with ints, try to stuff things into unboxed references as much as possible
--
-- TODO Custom fancy CMM wizardry
--  Maybe some fancy, custom CMM for array writes and reads:
--    - write (or CAS, I suppose) wouldn't need to check array size to find card table (because we know sEGMENT_LENGTH)
--    - write might not need to mark cards at all (since all elements are sharing a cheap value before write)
--    - instead we want reader to possibly do that
--       - read might only do a single card marking *after* filling the correspinding segment of e.g. 128 elems
--       - or is there some sort of "block write" for boxed arrays (copyArray?) that could be used after advancing past a certain point which would trigger a card marking?
--          - PROBLEM: other readers behind us may not have read their value yet.
--          - what about the read that *ends* a cards block marks that card?
--    - or can we just make smaller size arrays and turn off cardmarking on write?
--    - But GC still has to traverse card bytes; can we redefine array (or newArray primop) so that it has no/fewer card bits? 
--       see: https://ghc.haskell.org/trac/ghc/ticket/650#comment:17
--    https://ghc.haskell.org/trac/ghc/ticket/650

-- Back-of-envelope (time_to_create_new_segment / time_for_read_IOref) + margin.
-- See usage site.
rEADS_FOR_SEGMENT_CREATE_WAIT :: Int
rEADS_FOR_SEGMENT_CREATE_WAIT = round (((14.6::Float) + 0.3*fromIntegral sEGMENT_LENGTH) / 3.7) + 10

data InChan a = InChan !(Ticket (Cell a)) !(ChanEnd a)
newtype OutChan a = OutChan (ChanEnd a)
-- TODO derive Eq & Typeable

-- TODO POTENTIAL CPP FLAGS (or functions)
--   - Strict element (or lazy? maybe also reveal a writeChan' when relevant?)
--   - sEGMENT_LENGTH
--   - reads that clear the element immediately (or export as a special function?)

-- InChan & OutChan are mostly identical, sharing a stream, but with
-- independent counters
data ChanEnd a = 
           -- an efficient producer of segments of length sEGMENT_LENGTH:
    ChanEnd !(SegSource a)
            -- Both Chan ends must start with the same counter value.
            -- TODO maybe need a "generation" counter in case of crazy delayed thread (possible?)
            !AtomicCounter 
            -- the stream head; this must never point to a segment whose offset
            -- is greater than the counter value
            !(IORef (Stream a))

--TODO later see if we get a benefit from the small array primops in 7.10,
--     which omit card-marking overhead and might have faster clone.
type StreamSegment a = P.MutableArray RealWorld (Cell a)

-- TRANSITIONS and POSSIBLE VALUES:
--   During Read:
--     Empty   -> Blocking  (-> BlockedAborted)
--     Written
--   During Write:
--     Empty   -> Written 
--     Blocking
--     BlockedAborted
data Cell a = Empty | Written a | Blocking (MVar a) | BlockedAborted -- TODO !(Mvar a)
                                                                     -- TODO !a (maybe make optional)
                                                                     -- TODO ptr tagging? try combining these constructors

--   TODO:
--     However, then we have to consider how more elements will be kept around,
--     unable to be GC'd until we move onto the next segment. Solutions
--       - make read also CAS the element back to Empty (downside: slower reads)
--       - make the write strict to avoid some space-leaks at least 
--         - similarly, having Cell be an unboxed strict field.
--       - re-write writeArray CMM to avoid card-marking for a 'writeArray undefined'.
--
-- Constant for now: back-of-envelope considerations:
--   - making most of constant factor for cloning array of *any* size
--   - make most of overheads of moving to the next segment, etc.
--   - provide enough runway for creating next segment when 32 simultaneous writers 
--   - the larger this the larger one-time cost for the lucky reader/writer
--   - as arrays collect in heap, performance will be destroyed:  
--       http://stackoverflow.com/q/23462004/176841
sEGMENT_LENGTH :: Int
{-# INLINE sEGMENT_LENGTH #-}
sEGMENT_LENGTH = 1024 -- TODO Finalize this default.
-- NOTE In general we'll have two segments allocated at any given time in
-- addition to the segment template, so in the worst case, when the program
-- exits we will have allocated ~ 3 segments extra memory than was actually
-- required.

data Stream a = 
           -- the offset into the stream of this StreamSegment[0]
    Stream !Int 
           !(StreamSegment a)
           -- The next segment in the stream; new segments are allocated and
           -- put here as we go, with threads cooperating to allocate new
           -- segments:
           !(IORef (NextSegment a))

data NextSegment a = NoSegment | Next !(Stream a) -- TODO or Maybe? Does unboxing strict matter here?

-- we expose `startingCellOffset` for debugging correct behavior with overflow:
newChanStarting :: Int -> IO (InChan a, OutChan a)
{-# INLINE newChanStarting #-}
newChanStarting startingCellOffset = do
    let firstCount = startingCellOffset - 1
    segSource <- newSegmentSource
    firstSeg <- segSource
    -- collect a ticket to save for writer CAS
    savedEmptyTkt <- readArrayElem firstSeg 0
    stream <- Stream startingCellOffset firstSeg <$> newIORef NoSegment
    let end = ChanEnd segSource <$> newCounter firstCount <*> newIORef stream
    liftA2 (,) (InChan savedEmptyTkt <$> end) (OutChan <$> end)


-- TODO could we replace the mask_ with an exception-handler (would that be faster?)
writeChan :: InChan a -> a -> IO ()
{-# INLINE writeChan #-}
writeChan c@(InChan savedEmptyTkt ce@(ChanEnd segSource _ _)) a = mask_ $ do
    (segIx, str@(Stream _ segment _)) <- moveToNextCell ce
 -- NOTE: at this point we have an obligation to the reader of the assigned
 -- cell and what follows must already have async exceptions masked:
    (success,nonEmptyTkt) <- casArrayElem segment segIx savedEmptyTkt (Written a)
    if success 
         -- maybe optimistically try to set up next segment.
        then advanceStreamIfFirst segIx str segSource
        else case peekTicket nonEmptyTkt of
                  Blocking v -> putMVar v a
                  -- a reader was killed (async exception) such that if `a`
                  -- disappeared this might be observed to differ from the
                  -- semantics of Chan. Retry:
                  BlockedAborted -> writeChan c a
                  _ ->
            -- Theoretical race condition: Writer reads head, then descheduled.
            -- Millions of segments are allocated and counter incremented but
            -- none manage to move the head pointer. When the counter comes
            -- back around, the next writer assigned this count will write to
            -- the cell we've been assigned and this error will be raised.
                      error "Nearly Impossible! Expected Blocking or BlockedAborted"
        -- TODO A WORSE CASE: another descheduled thread from this same block
        --      completes, moving the pointer way backwards.
        -- HOW TO SOLVE?
        --     We want to move the pointer forward only
        --      - could use "generations"
        --         - would we have to deal with this anywhere else?
        --         - if not, then probably worth it.
        --      - use heuristic, or just only move if within some forward window
        --         maybe just subtract and only move if positive, then do?
  -- [2] the writer or reader which arrives first to the first cell of a new
  -- segment is tasked (somewhat arbitrarily) with trying to pre-allocate the
  -- *next* segment hopefully ahead of any readers or writers who might need
  -- it. This will race with any reader or writer that tries to read the next
  -- segment and finds it's empty; when this wins (hopefully the vast majority
  -- of the time) we avoid a throughput hit.
  --
  -- [1] Reader may have not set up the next segment, but we don't try here in
  -- the writer, even if segIx == 0


readChan :: OutChan a -> IO a
-- TODO consider removing this mask_ (does result in a performance improvement)
--      maybe providing an alternative function.
{-# INLINE readChan #-}
readChan (OutChan ce@(ChanEnd segSource _ _)) = mask_ $ do  -- NOTE [1]
    (segIx, str@(Stream _ segment _)) <- moveToNextCell ce
    cellTkt <- readArrayElem segment segIx
    case peekTicket cellTkt of
         Written a -> return a
         Empty -> do
            v <- newEmptyMVar
            (success,elseWrittenCell) <- casArrayElem segment segIx cellTkt (Blocking v)
            if success 
              then do 
                -- if we're the first of the segment and about to do a blocking
                -- read (i.e.  we beat writer to cell 0) then optimistically
                -- set up the next segment first:
                advanceStreamIfFirst segIx str segSource
                -- Block, waiting for the future writer. -- NOTE [2]
                takeMVar v `onException` (
                  P.writeArray segment segIx BlockedAborted )  -- NOTE [3]
              -- In the meantime a writer has written. Good!
              else case peekTicket elseWrittenCell of
                        Written a -> return a
                        _ -> error "Impossible! Expecting Written"
         _ -> error "Impossible! Only expecting Empty or Written"
  -- [1] we want our queue to behave like Chan in that (reasoning in terms of
  -- linearizability) the first reader waiting in line for an element may lose
  -- that element for good when an async exception is raised (when the read and
  -- write overlap); but an async exception raised in a reader "blocked in
  -- line" (i.e. which blocked after the first blocked reader) should never
  -- result in a lost message.  To that end we mask this, and on exception while
  -- blocking we write `BlockedAborted`, signaling writer to retry.
  --
  -- [2] When a reader is blocked and an async exception is raised we risk losing
  -- the next written element. To avoid this our handler writes BlockedAborted to
  -- signal the next writer to retry. Unfortunately we can't force a `throwTo` to
  -- block until our handler *finishes* (see trac #9030) thus introducing a race
  -- condition that could cause a thread to observe different semantics to Chan.
  --
  -- [3] re. writeArray race condition: if read overlaps with write, this is
  -- undefined behavior anyway:


-- increments counter, finds stream segment of corresponding cell (updating the
-- stream head pointer as needed), and returns the stream segment and relative
-- index of our cell.
moveToNextCell :: ChanEnd a -> IO (Int, Stream a)
{-# INLINE moveToNextCell #-}
moveToNextCell (ChanEnd segSource counter streamHead) = do
    str0@(Stream offset0 _ _) <- readIORef streamHead
    -- !!! TODO BARRIER REQUIRED FOR NON-X86 !!!
    ix <- incrCounter 1 counter
    let (segsAway, segIx) = assert ((ix - offset0) >= 0) $ 
                 --(ix - offset0) `quotRem` sEGMENT_LENGTH
                 divMod_sEGMENT_LENGTH $! (ix - offset0)
        waitSpins = rEADS_FOR_SEGMENT_CREATE_WAIT*segIx -- NOTE [1]
        {-# INLINE go #-}
        go 0 str = return str
        go !n str@(Stream offset _ next) =
            waitingAdvanceStream next offset segSource waitSpins
              >>= go (n-1)
    str <- go segsAway str0
    when (segsAway > 0) $ writeIORef streamHead str -- NOTE [2]
    return (segIx,str)
  -- [1] All readers or writers needing to work with a not-yet-created segment
  -- race to create it, but those past index 0 have progressively long waits; 20
  -- is chosen as 20 readIORefs should be more than enough time for writer/reader
  -- 0 to add the new segment (if it's not descheduled).
  --
  -- [2] advancing the stream head pointer on segIx == sEGMENT_LENGTH - 1 would
  -- be more correct, but this is simpler here. This may move the head pointer
  -- *backwards* if the thread was descheduled, but that's not a correctness
  -- issue. TODO check we're not moving the head pointer backwards; maybe need a generation counter + handle overflow correctly.


advanceStreamIfFirst :: Int -> Stream a -> SegSource a -> IO ()
advanceStreamIfFirst segIx (Stream offset _ next) segSource = 
    when (segIx == 0) $ void $
        waitingAdvanceStream next offset segSource 0

-- thread-safely try to fill `nextSegRef` at the next offset with a new
-- segment, waiting some number of iterations (for other threads to handle it).
-- Returns nextSegRef's StreamSegment.
waitingAdvanceStream :: IORef (NextSegment a) -> Int -> SegSource a 
                     -> Int -> IO (Stream a)
waitingAdvanceStream nextSegRef prevOffset segSource = go where
  go !wait = assert (wait >= 0) $ do
    tk <- readForCAS nextSegRef
    case peekTicket tk of
         NoSegment 
           | wait > 0 -> go (wait - 1)
             -- Create a potential next segment and try to insert it:
           | otherwise -> do 
               potentialStrNext <- Stream (prevOffset + sEGMENT_LENGTH) 
                                      <$> segSource 
                                      <*> newIORef NoSegment
               (_,tkDone) <- casIORef nextSegRef tk (Next potentialStrNext)
               -- If that failed another thread succeeded (no false negatives)
               case peekTicket tkDone of
                 Next strNext -> return strNext
                 _ -> error "Impossible! This should only have been Next segment"
         Next strNext -> return strNext
  -- TODO clean up and need some tests / assertions


-- copying a template array with cloneMutableArray is much faster than creating
-- a new one; in fact it seems we need this in order to scale, since as cores
-- increase we don't have enough "runway" and can't allocate fast enough:
type SegSource a = IO (StreamSegment a)

newSegmentSource :: IO (SegSource a)
{-# INLINE newSegmentSource #-}
newSegmentSource = do
    -- TODO check that Empty is actually shared (or however that works with
    --      single-constructors)
    arr <- P.newArray sEGMENT_LENGTH Empty
    return (P.cloneMutableArray arr 0 sEGMENT_LENGTH)

-- ----------
-- CELLS AND GC:
--
--   Each cell in a segment is assigned at most one reader and one writer
--
--   When all readers disappear and writers continue we'll have at most one
--   segment-worth of garbage that can't be collected at a time; when writers
--   advance the head segment pointer, the previous may be GC'd.
--
--   Readers blocked indefinitely should eventually raise a
--   BlockedIndefinitelyOnMVar.
-- ----------



-- JUST FOR DEBUGGING / TESTING FOR NOW --


newtype ThreadKilledChan = ThreadKilledChan (MVar ())
    deriving (Typeable)
instance Show ThreadKilledChan where
    show _ = "ThreadKilledChan"
instance Exception ThreadKilledChan

throwKillTo :: ThreadId -> IO ()
throwKillTo tid = do
    v <- newEmptyMVar
    throwTo tid (ThreadKilledChan v)
    takeMVar v

catchKillRethrow :: IO a -> IO a
catchKillRethrow io = 
   io `E.catches` [
             Handler $ \(ThreadKilledChan bvar)-> do
               putMVar bvar () -- unblocking thrower
               throwIO ThreadKilled
           , Handler $ \e-> do
               throwIO (e :: SomeException) 
           ] 


pOW, sEGMENT_LENGTH_MN_1 :: Int
pOW = round $ logBase 2 $ fromIntegral sEGMENT_LENGTH -- or bit shifts in loop
sEGMENT_LENGTH_MN_1 = sEGMENT_LENGTH - 1

divMod_sEGMENT_LENGTH :: Int -> (Int,Int)
{-# INLINE divMod_sEGMENT_LENGTH #-}
-- divMod_sEGMENT_LENGTH n = ( n `unsafeShiftR` pOW, n .&. sEGMENT_LENGTH_MN_1)
divMod_sEGMENT_LENGTH n = let d = n `unsafeShiftR` pOW
                              m = n .&. sEGMENT_LENGTH_MN_1
                           in d `seq` m `seq` (d,m)

{- TESTS SKETCH
 - validate with some benchmarks
 - look over implementation for other assertions / micro-tests
 - Make sure we never get False returned on casIORef where we no no conflicts, i.e. no false negatives
     - also include arbitrary delays between readForCAS and the CAS
 - (Not a test, but...) add a branch with a whole load of event logging that we can analyze (maybe in an automated test!)
 - perhaps run num_threads readers and writers, plus some threads that can inspect the queues
    for bad descheduling conditions we want to avoid.
 - use quickcheck to generate 'new' chans that represent possible conditions and test those with write/read (or with our regular test suite?)
    - we also want to test counter roll-over
 -}

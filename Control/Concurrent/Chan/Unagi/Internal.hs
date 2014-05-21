{-# LANGUAGE BangPatterns , DeriveDataTypeable #-}
module Control.Concurrent.Chan.Unagi.Internal (
     sEGMENT_LENGTH
    , InChan(..), OutChan(..), ChanEnd(..), StreamSegment, Cell(..), Stream(..)
    , NextSegment(..), StreamHead(..)
    , newChanStarting, writeChan, readChan, readChanOnException
    )
    where

-- Internals exposed for testing.

import Control.Concurrent.MVar
import Data.IORef
-- import Control.Exception(mask_,assert,onException,throwIO,SomeException,Exception)
import Control.Exception

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

-- Number of reads on which to spin for new segment creation.
-- Back-of-envelope (time_to_create_new_segment / time_for_read_IOref) + margin.
-- See usage site.
nEW_SEGMENT_WAIT :: Int
nEW_SEGMENT_WAIT = round (((14.6::Float) + 0.3*fromIntegral sEGMENT_LENGTH) / 3.7) + 10

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
            !AtomicCounter 
            -- the stream head; this must never point to a segment whose offset
            -- is greater than the counter value
            !(IORef (StreamHead a))

data StreamHead a = StreamHead !Int !(Stream a)

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
data Cell a = Empty | Written a | Blocking !(MVar a)

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
--
-- NOTE: THIS REMAIN A POWER OF 2!
sEGMENT_LENGTH :: Int
{-# INLINE sEGMENT_LENGTH #-}
sEGMENT_LENGTH = 1024 -- TODO Finalize this default.

-- NOTE In general we'll have two segments allocated at any given time in
-- addition to the segment template, so in the worst case, when the program
-- exits we will have allocated ~ 3 segments extra memory than was actually
-- required.

data Stream a = 
    Stream !(StreamSegment a)
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
    stream <- Stream firstSeg <$> newIORef NoSegment
    let end = ChanEnd segSource 
                  <$> newCounter firstCount 
                  <*> newIORef (StreamHead startingCellOffset stream)
    liftA2 (,) (InChan savedEmptyTkt <$> end) (OutChan <$> end)


-- TODO add benchmark that does a mapM_ (writeChan c), and test whether moving \a-> to RHS helps w/ inlining
-- TODO add benchmark that sends and uses Ints to test whether unboxed-strict elements in Cell help
writeChan :: InChan a -> a -> IO ()
{-# INLINE writeChan #-}
writeChan (InChan savedEmptyTkt ce@(ChanEnd segSource _ _)) a = mask_ $ do
    (segIx, (Stream segment next)) <- moveToNextCell ce
    (success,nonEmptyTkt) <- casArrayElem segment segIx savedEmptyTkt (Written a)
    -- try to pre-allocate next segment; NOTE [1]
    when (segIx == 0) $ void $
      waitingAdvanceStream next segSource 0
    when (not success) $
        case peekTicket nonEmptyTkt of
             Blocking v -> putMVar v a
             -- a reader was killed (async exception) such that if `a`
             -- disappeared this might be observed to differ from the
             -- semantics of Chan. Retry:
             _ -> error "Nearly Impossible! Expected Blocking"
  -- [1] the writer which arrives first to the first cell of a new segment is
  -- tasked (somewhat arbitrarily) with trying to pre-allocate the *next*
  -- segment hopefully ahead of any readers or writers who might need it. This
  -- will race with any reader *or* writer that tries to read the next segment
  -- and finds it's empty (see `waitingAdvanceStream`); when this wins
  -- (hopefully the vast majority of the time) we avoid a throughput hit.


-- We would like our queue to behave like Chan in that an async exception
-- raised in a reader known to be blocked or about to block on an empty queue
-- never results in a lost message; this matches our simple intuition about the
-- mechanics of a queue: if you're last in line for a cupcake and have to leave
-- you wouldn't expect the cake that would have gone to you to disappear.
--
-- But there are other systems that a busy cake shop might use that you could
-- easily imagine resulting in a lost cake, and that's a different question
-- from whether cakes are given to well-behaved customers in the order they
-- came out of the oven, or whether a customer leaving at the wrong moment
-- might cause the cake shop to burn down...
readChanOnExceptionUnmasked :: (IO a -> IO ()) -> OutChan a -> IO a
{-# INLINE readChanOnExceptionUnmasked #-}
readChanOnExceptionUnmasked h = \(OutChan ce)-> do
    (segIx, (Stream segment _)) <- moveToNextCell ce
    cellTkt <- readArrayElem segment segIx
    case peekTicket cellTkt of
         Written a -> return a
         Empty -> do
            v <- newEmptyMVar
            (success,elseWrittenCell) <- casArrayElem segment segIx cellTkt (Blocking v)
            if success 
              then do 
                 -- TODO consider using readMVar on GHC 7.8:
                -- Block, waiting for the future writer.
                takeMVar v `onException` (
                  h (takeMVar v) )
              -- In the meantime a writer has written. Good!
              else case peekTicket elseWrittenCell of
                        Written a -> return a
                        _ -> error "Impossible! Expecting Written"
         _ -> error "Impossible! Only expecting Empty or Written"


-- | Read an element from the chan, blocking if the chan is empty.
--
-- /Note re. exceptions/: When an async exception is raised during a @readChan@ 
-- the message that the read would have returned is likely to be lost, even when
-- the read is known to be blocked on an empty queue. If you need to handle
-- this scenario, you can use 'readChanOnException'.
readChan :: OutChan a -> IO a
{-# INLINE readChan #-}
readChan = readChanOnExceptionUnmasked void

-- | Like 'readChan' but allows recovery of the queue element which would have
-- been read, in the case that an async exception is raised during the read. To
-- be precise exceptions are raised, and the handler run, only when
-- @readChanOnException@ is blocking on an empty queue.
--
-- The second argument is a handler that takes a blocking IO action returning
-- the element, and performs some recovery action.  When the handler is called,
-- the passed @IO a@ is the only way to access the element.
readChanOnException :: OutChan a -> (IO a -> IO ()) -> IO a
{-# INLINE readChanOnException #-}
readChanOnException c h = mask_ $ readChanOnExceptionUnmasked h c

-- increments counter, finds stream segment of corresponding cell (updating the
-- stream head pointer as needed), and returns the stream segment and relative
-- index of our cell.
moveToNextCell :: ChanEnd a -> IO (Int, Stream a)
{-# INLINE moveToNextCell #-}
moveToNextCell (ChanEnd segSource counter streamHead) = do
    (StreamHead offset0 str0) <- readIORef streamHead
    -- NOTE [3]
    -- !!! TODO BARRIER REQUIRED FOR NON-X86 !!!
    ix <- incrCounter 1 counter
    let (segsAway, segIx) = assert ((ix - offset0) >= 0) $ 
                 divMod_sEGMENT_LENGTH $! (ix - offset0)
              -- (ix - offset0) `quotRem` sEGMENT_LENGTH
        {-# INLINE go #-}
        go 0 str = return str
        go !n (Stream _ next) =
            waitingAdvanceStream next segSource (nEW_SEGMENT_WAIT*segIx) -- NOTE [1]
              >>= go (n-1)
    str <- go segsAway str0
    -- TODO would a one-shot CAS be better here? Test.
    when (segsAway > 0) $ do
        let !offsetN = 
              offset0 + (segsAway `unsafeShiftL` pOW) --(segsAway*sEGMENT_LENGTH)
        writeIORef streamHead $ StreamHead offsetN str -- NOTE [2]
    return (segIx,str)
  -- [1] All readers or writers needing to work with a not-yet-created segment
  -- race to create it, but those past index 0 have progressively long waits; 20
  -- is chosen as 20 readIORefs should be more than enough time for writer/reader
  -- 0 to add the new segment (if it's not descheduled).
  --
  -- [2] advancing the stream head pointer on segIx == sEGMENT_LENGTH - 1 would
  -- be more correct, but this is simpler here. This may move the head pointer
  -- *backwards* if the thread was descheduled, but that's not a correctness
  -- issue.
  --
  -- [3] There is a theoretical race condition here: thread reads head and is
  -- descheduled, meanwhile other readers/writers increment counter one full
  -- lap; when we increment we think we've found our cell in what is actually a
  -- very old segment. However in this scenario all addressable memory will
  -- have been consumed just by the array pointers whivh haven't been able to
  -- be GC'd. So I don't think this is something to worry about.


-- thread-safely try to fill `nextSegRef` at the next offset with a new
-- segment, waiting some number of iterations (for other threads to handle it).
-- Returns nextSegRef's StreamSegment.
waitingAdvanceStream :: IORef (NextSegment a) -> SegSource a 
                     -> Int -> IO (Stream a)
waitingAdvanceStream nextSegRef segSource = go where
  go !wait = assert (wait >= 0) $ do
    tk <- readForCAS nextSegRef
    case peekTicket tk of
         NoSegment 
           | wait > 0 -> go (wait - 1)
             -- Create a potential next segment and try to insert it:
           | otherwise -> do 
               potentialStrNext <- Stream <$> segSource 
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
newSegmentSource = do
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




pOW, sEGMENT_LENGTH_MN_1 :: Int
pOW = round $ logBase (2::Float) $ fromIntegral sEGMENT_LENGTH -- or bit shifts in loop
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

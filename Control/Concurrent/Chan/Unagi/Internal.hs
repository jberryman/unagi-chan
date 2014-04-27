{-# LANGUAGE BangPatterns , DeriveDataTypeable #-}
module Control.Concurrent.Chan.Unagi.Internal
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


-- approx time_to_create_new_segment / time_for_read_IOref, + some margin. See
-- usage site.
rEADS_FOR_SEGMENT_CREATE_WAIT :: Int
rEADS_FOR_SEGMENT_CREATE_WAIT = 20

newtype InChan a = InChan (ChanEnd a)
newtype OutChan a = OutChan (ChanEnd a)
-- TODO derive Eq & Typeable


-- InChan & OutChan are identical, sharing a stream, but with independent
-- counters
data ChanEnd a = 
           -- an efficient producer of segments of length sEGMENT_LENGTH:
    ChanEnd !(SegSource a)
            -- Both Chan ends must start with the same counter value.
            -- TODO maybe need a "generation" counter in case of crazy delayed thread (possible?)
            !AtomicCounter 
            -- the stream head; this must never point to a segment whose offset
            -- is greater than the counter value
            !(IORef (Stream a))

type StreamSegment a = P.MutableArray RealWorld (Cell a)
-- TRANSITIONS:
--   During Read:
--     Empty   -> Blocking  (-> BlockedAborted)
--     Written
--   During Write:
--     Empty   -> Written 
--     Blocking
--     BlockedAborted
data Cell a = Empty | Written a | Blocking (MVar a) | BlockedAborted

-- Constant for now: back-of-envelope considerations:
--   - making most of constant factor for cloning array of *any* size
--   - make most of overheads of moving to the next segment, etc.
--   - provide enough runway for creating next segment when 32 simultaneous writers 
sEGMENT_LENGTH :: Int
sEGMENT_LENGTH = 64
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
    stream <- Stream startingCellOffset <$> segSource <*> newIORef NoSegment
    let end = ChanEnd segSource <$> newCounter firstCount <*> newIORef stream
    liftA2 (,) (InChan <$> end) (OutChan <$> end)

writeChan :: InChan a -> a -> IO ()
{-# INLINABLE writeChan #-}
writeChan c@(InChan ce@(ChanEnd segSource _ _)) a = mask_ $ do
    (segIx, str@(Stream _ segment _)) <- moveToNextCell ce
 -- NOTE: at this point we have an obligation to the reader of the assigned
 -- cell and what follows must have async exceptions masked:
    maybeWroteFirst <- writeCell segment segIx a
    case maybeWroteFirst of 
         -- retry if cell reader is seen to have been killed:
         Nothing -> writeChan c a  -- NOTE [1]
         -- maybe optimistically try to set up next segment.
         Just wroteFirst -> when (segIx == 0 && wroteFirst) $ -- NOTE [2]
                              advanceStream str segSource
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
{-# INLINABLE readChan #-}
readChan (OutChan ce@(ChanEnd segSource _ _)) = mask_ $ do  -- NOTE [1]
    (segIx, str@(Stream _ segment _)) <- moveToNextCell ce
    futureA <- readCell segment segIx
    case futureA of
         Left a -> return a
         Right blockingReturn_a -> do
           -- if we're the first of the segment and about to do a blocking read
           -- (i.e.  we beat writer to cell 0) then we optimistically set up
           -- the next segment first, then perform that blocking read:
           when (segIx == 0) $ advanceStream str segSource
           blockingReturn_a
  -- [1] we want our queue to behave like Chan in that (reasoning in terms of
  -- linearizability) the first reader waiting in line for an element may lose
  -- that element for good when an async exception is raised (when the read and
  -- write overlap); but an async exception raised in a reader "blocked in
  -- line" (i.e. which blocked after the first blocked reader) should never
  -- result in a lost message.  To that end we mask this, and on exception while
  -- blocking we write `BlockedAborted`, signaling writer to retry.


-- increments counter, finds stream segment of corresponding cell (updating the
-- stream head pointer as needed), and returns the stream segment and relative
-- index of our cell.
moveToNextCell :: ChanEnd a -> IO (Int, Stream a)
{-# INLINE moveToNextCell #-}
moveToNextCell (ChanEnd segSource counter streamHead) = do
    str0@(Stream offset0 _ _) <- readIORef streamHead
    -- !!! TODO BARRIER REQUIRED FOR NON-X86 !!!
    ix <- incrCounter 1 counter
    let go str@(Stream offset _ next) =
          let !segIx = ix - offset
           in assert (segIx >= 0) $ -- else cell is unreachable!
              if segIx < sEGMENT_LENGTH
                -- assigned cell is within this segment
                then do 
                  when (segIx == 0 && offset /= offset0) $  -- NOTE [2]
                    writeIORef streamHead str 
                  return (segIx , str)
                else waitingAdvanceStream next offset segSource (20*segIx) -- NOTE [1]
                      >>= go
    go str0
  -- [1] All readers or writers needing to work with a not-yet-created segment
  -- race to create it, but those past index 0 have progressively long waits; 20
  -- is chosen as 20 readIORefs should be more than enough time for writer/reader
  -- 0 to add the new segment (if it's not descheduled).
  --
  -- [2] advancing the stream head pointer on segIx == sEGMENT_LENGTH - 1 would
  -- be more correct, but this is simpler here. This may move the head pointer
  -- *backwards* if the thread was descheduled, but that's not a correctness
  -- issue. TODO check we're not moving the head pointer backwards; maybe need a generation counter + handle overflow correctly.


advanceStream :: Stream a -> SegSource a -> IO ()
{-# INLINE advanceStream #-}
advanceStream (Stream offset _ next) segSource = void $
    waitingAdvanceStream next offset segSource (0::Int)

-- thread-safely try to fill `nextSegRef` at the `nextOffset` with a new
-- segment, waiting some number of iterations (for other threads to handle it).
-- Returns nextSegRef's StreamSegment.
--
waitingAdvanceStream :: IORef (NextSegment a) -> Int -> SegSource a 
                     -> Int -> IO (Stream a)
{-# INLINE waitingAdvanceStream #-}
waitingAdvanceStream nextSegRef prevOffset segSource = go where
  go !wait = assert (wait >= 0) $ do
    tk <- readForCAS nextSegRef
    case peekTicket tk of
         NoSegment 
           | wait > 0 -> go (wait - 1)
             -- create a potential next segment and try to insert it:
           | otherwise -> do 
               let nextOffset = prevOffset + sEGMENT_LENGTH
               potentialStrNext <- Stream nextOffset <$> segSource <*> newIORef NoSegment
               (_,tkDone) <- casIORef nextSegRef tk (Next potentialStrNext)
               -- that may have failed (meaning another thread took care of
               -- it); regardless next segment must now be there:
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

-- Once we have the stream segment to which we've been assigned, write to our
-- assigned index. returns Nothing if we have to retry, otherwise a Bool
-- indicating whether we beat any waiting readers.
writeCell :: StreamSegment a -> Int -> a -> IO (Maybe Bool)
{-# INLINE writeCell #-}
writeCell arr ix a = do
    cellTkt <- readArrayElem arr ix
    case peekTicket cellTkt of
         Empty -> do (success,cellTkt') <- casArrayElem arr ix cellTkt (Written a)
                     if success 
                         then return (Just True)
                         else handleNotEmpty (peekTicket cellTkt')
         cell -> handleNotEmpty cell

  where handleNotEmpty (Blocking v)   = putMVar v a >> return (Just False)
        -- a reader was killed (async exception) such that if `a` disappeared
        -- this might be observed to differ from the semantics of Chan. Retry:
        handleNotEmpty BlockedAborted = return Nothing
        handleNotEmpty _ 
            -- Theoretical race condition: Writer reads head, then descheduled.
            -- Millions of segments are allocated and counter incremented but
            -- none manage to move the head pointer. When the counter comes
            -- back around, the next writer assigned this count will write to
            -- the cell we've been assigned and this error will be raised.
            = error "Nearly Impossible! Expected Blocking or BlockedAborted"
        -- TODO A WORSE CASE: another descheduled thread from this same block
        --      completes, moving the pointer way backwards.
        -- HOW TO SOLVE?
        --     We want to move the pointer forward only
        --      - could use "generations"
        --         - would we have to deal with this anywhere else?
        --         - if not, then probably worth it.
        --      - use heuristic, or just only move if within some forward window
        --         maybe just subtract and only move if positive, then do?


-- ...and likewise for the reader. Each index of each segment has at most one
-- reader and one writer assigned by the atomic counters. We return either the
-- read element or a blocking IO "future" to be processed later by caller
readCell :: StreamSegment a -> Int -> IO (Either a (IO a))
{-# INLINE readCell #-}
readCell arr ix = do
    cellTkt <- readArrayElem arr ix
    case peekTicket cellTkt of
         Written a -> return $ Left a
         Empty -> do
            v <- newEmptyMVar
            (success,elseWrittenCell) <- casArrayElem arr ix cellTkt (Blocking v)
            return $
              if success 
                -- Block, waiting for the future writer. -- NOTE [1]
                then Right (takeMVar v `onException` (
                       -- re. writeArray race condition: if read overlaps with
                       -- write, this is undefined behavior anyway:
                       P.writeArray arr ix BlockedAborted ) ) -- NOTE [2]
                -- In the meantime a writer has written. Good!
                else case peekTicket elseWrittenCell of
                          Written a -> Left a
                          _ -> error "Impossible! Expecting Written"
         _ -> error "Impossible! Only expecting Empty or Written"

-- [1] When a reader is blocked and an async exception is raised we risk losing
-- the next written element. To avoid this our handler writes BlockedAborted to
-- signal the next writer to retry. Unfortunately we can't force a `throwTo` to
-- block until our handler *finishes* (see trac #9030) thus introducing a race
-- condition that could cause a thread to observe different semantics to Chan.
--
-- [2] re. writeArray race condition: if read overlaps with write, this is
-- undefined behavior anyway:



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


{- TESTS SKETCH
 - validate with some benchmarks
 - look over implementation for other assertions / micro-tests
 - make sure that first write after chan creation goes into [0] and that specified initial offset was correct.
 - Make sure we never get False returned on casIORef where we no no conflicts, i.e. no false negatives
     - also include arbitrary delays between readForCAS and the CAS
 - (Not a test, but...) add a branch with a whole load of event logging that we can analyze (maybe in an automated test!)
 - perhaps run num_threads readers and writers, plus some threads that can inspect the queues
    for bad descheduling conditions we want to avoid.
 - use quickcheck to generate 'new' chans that represent possible conditions and test those with write/read (or with our regular test suite?)
    - we also want to test counter roll-over
 - actual 'smoke' unit test, single-threaded write some/read, with different initial counter offsets.
 -}

module Control.Concurrent.Chan.Unagi.Internal
    where

-- Internals exposed for testing.
import Control.Concurrent.MVar
import Data.IORef
import Control.Exception(evaluate,mask_,assert)

import Control.Monad.Primitive(PrimState)
import Data.Atomics.Counter
import Data.Atomics
import qualified Data.Primitive as P

import Control.Applicative


type StreamSegment a = P.MutableArray RealWorld (Cell a)

-- Constant for now: back-of-envelope considerations:
--   - making most of constant factor for cloning array of *any* size
--   - make most of overheads of moving to the next segment, etc.
--   - provide enough runway for creating next segment when 32 simultaneous writers 
sEGMENT_LENGTH :: Int
sEGMENT_LENGTH = 64

-- approx time_to_create_new_segment / time_for_read_IOref, + some margin. See
-- usage site.
rEADS_FOR_SEGMENT_CREATE_WAIT :: Int
rEADS_FOR_SEGMENT_CREATE_WAIT = 20

-- expose for debugging correct behavior with overflow:
sTARTING_CELL_OFFSET :: Int
sTARTING_CELL_OFFSET = minBound


-- TRANSITIONS::
--   During Read:
--     Empty   -> Blocking  (-> BlockedAborted)
--     Written
--   During Write:
--     Empty   -> Written 
--     Blocking
--     BlockedAborted
data Cell a = Empty | Written a | Blocking (MVar a) | BlockedAborted

-- NOTE In general we'll have two segments allocated at any given time in
-- addition to the segment template, so in the worst case, when the program
-- exits we will have allocated ~ 3 segments extra memory than was actually
-- required.
-- TODO: any way to manually tell the GC we don't need a cell? Benefits or
--       hurts performance?

data Stream a = 
           -- the offset into the stream of this segment; the reader/writer
           -- receiving a counter value of offset operates on segment[0] 
    Stream !Int 
           (!StreamSegment a)
           -- The next segment in the stream; new segments are allocated and
           -- put here as we go, with threads cooperating to allocate new
           -- segments:
           (!IORef (NextSegment a))

data NextSegment a = NoSegment | Next !(Stream a) -- TODO or Maybe? Does unboxing strict matter here?
nullSegment :: NextSegment a -> Bool
nullSegment NoSegment = True
nullSegment _ = False

-- TODO RENAME ChanEnd
data InChan a = 
           -- an efficient producer of segments of length sEGMENT_LENGTH:
    InChan (!IO (StreamSegment a))
           -- Must start at same value of 
           -- TODO maybe need a "generation" counter in case of crazy delayed thread (possible?)
           !AtomicCounter 
           -- the stream head; this must never point to a segment whose offset
           -- is greater than the counter value
           (!IORef (Stream a))

-- TODO make these Chan, with newtype wrappers.
data OutChan a = OutChan AtomicCounter


newSplitChan :: IO (InChan a, OutChan a)
{-# INLINABLE newSplitChan #-}
newSplitChan = do
    segSource <- newSegmentSource
    InChan segSource

writeChan :: InChan a -> a -> IO ()
{-# INLINABLE writeChan #-}
writeChan c@(InChan segSource counter streamHead) a = mask_ $ do
 -- NOTE: at this point we have an obligation to the reader of the assigned
 -- cell and what follows must have async exceptions masked:
    (segIx, str@(Stream offset segment next)) <- moveToNextCell c
    maybeWroteFirst <- writeCell segment segIx a
    case maybeWroteFirst of 
         -- retry if cell reader is seen to have been killed:
         Nothing -> writeChan c a  -- NOTE [1]
         -- maybe optimistically try to set up next segment.
         Just wroteFirst -> when (segIx == 0 && wroteFirst) $ -- NOTE [2]
            --  TODO make sure this handles case where already set up
            void $ waitingAdvanceStream next offset segSource 0
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
readChan c@(InChan segSource counter streamHead) = mask_ $ do  -- NOTE [1]
    (segIx, str@(Stream offset segment next)) <- moveToNextCell c
    futureA <- readCell segment segIx
    case futureA of
         Left a -> return a
         Right blockingReturn_a -> do
           -- if we're the first of the segment and about to do a blocking read
           -- (i.e.  we beat writer to cell 0) then we optimistically set up
           -- the next segment first, then perform that blocking read:
           when (segIx == 0) $ 
             waitingAdvanceStream next offset segSource 0
           blockingReturn_a
  -- [1] we want our queue to behave like Chan in that (reasoning in terms of
  -- linearizability) the first reader waiting in line for an element may lose
  -- that element for good when an async exception is raised (when the read and
  -- write overlap); but an async exception raised in a reader "blocked in
  -- line" (i.e. which blocked after the first blocked reader) should never
  -- result in a lost message.  To that end we mask this, and on exception while
  -- blocking we write `BlockedAborted`, signaling writer to retry.



moveToNextCell :: Chan a -> (Int, Stream a)
{-# INLINE moveToNextCell #-}
moveToNextCell (InChan segSource counter streamHead) = do
    str0@(Stream offset0 _ _) <- readIORef streamHead
    -- !!! TODO BARRIER MAYBE REQUIRED !!!
    ix <- incrCounter 1 counter

    let go str@(Stream offset _ next) = do
            let !segIx = ix - offset
            assert (segIx >= 0) $ -- else cell is unreachable!
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


-- TODO tighten this up: utility function for read_cas/case/cas (maybe
--      polymorphic, maybe using LambdaCase)?
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
                 NextSegment strNext -> return strNext
                 _ -> error "Impossible! This should only have been NextSegment"
         NextSegment strNext -> return strNext


-- copying a template array with cloneMutableArray is much faster than creating
-- a new one; in fact it seems we need this in order to scale, since as cores
-- increase we don't have enough "runway" and can't allocate fast enough:
newSegmentSource :: IO (IO (StreamSegment a))
{-# INLINE newSegmentSource #-}
newSegmentSource = do
    arr <- newArray sEGMENT_LENGTH Empty
    return (cloneMutableArray arr 0 sEGMENT_LENGTH)


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
         Empty -> do (success,cellTkt') <- casArrayElem arr ix cellTkt a
                     if success 
                         then return (Just True)
                         else handleNotEmpty (peekTicket cellTkt')
         cell -> handleNotEmpty cell

  where handleNotEmpty (Blocking v)   = putMVar v a >> return (Just False)
        -- a reader was killed (async exception) such that if `a` disappeared
        -- this might be observed to differ from the semantics of Chan. Retry:
        handleNotEmpty BlockedAborted = return Nothing
        handleNotEmpty _ 
            = error "Impossible! Expected Blocking or BlockedAborted"

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
                -- Block, waiting for the future writer.
                then Right (takeMVar v `onException` (
                       -- re. writeArray race condition: if read overlaps with
                       -- write, this is undefined behavior anyway:
                       writeArray arr ix BlockedAborted ) )
                -- In the meantime a writer has written. Good!
                else case peekTicket elseWrittenCell of
                          Written a -> Left a
                          _ -> error "Impossible! Expecting Written"
         _ -> error "Impossible! Only expecting Empty or Written"


{- TESTS SKETCH
 - Test these assumptions:
     1) If a CAS fails in thread 1 then another CAS (in thread 2, say) succeeded
     2) In the case that thread 1's CAS failed, the ticket returned with (False,tk) will contain that newly-written value from thread 2
 - record debugging events for:
     - next segment not pre-allocated
     - etc...
 - test with initial counter position at almost-rollover to test that.
 -}

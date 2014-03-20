module Control.Concurrent.Chan.Unagi.Internal
    where

-- Internals exposed for testing.
import Control.Concurrent.MVar
import Data.IORef
import Control.Exception(evaluate,mask_)

import Control.Monad.Primitive(PrimState)
import Data.Atomics.Counter
import Data.Atomics
import qualified Data.Primitive as P


type StreamSegment a = P.MutableArray RealWorld (Cell a)

-- Constant for now: back-of-envelope considerations:
--   - making most of constant factor for cloning array of *any* size
--   - make most of overheads of moving to the next segment, etc.
--   - provide enough runway for creating next when 32 simultaneous writers 
sEGMENT_LENGTH :: Int
sEGMENT_LENGTH = 64

-- approx time_to_create_new_segment / time_for_read_IOref, + some margin. See
-- usage site.
rEADS_FOR_SEGMENT_CREATE_WAIT :: Int
rEADS_FOR_SEGMENT_CREATE_WAIT = 20


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

data InChan a = 
           -- creates an efficient producer of segments of length
           -- sEGMENT_LENGTH:
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
writeChan (InChan segSource counter streamHead) a = mask_ $ do
    str0 <- readIORef streamHead
    -- !!! TODO BARRIER MAYBE REQUIRED !!!
 -- NOTE: after this point we have an obligation to the reader of the assigned
 -- cell and what follows must have async exceptions masked:
    ix <- incrCounter 1 counter
    findCellAndWrite ix a str0

-- TODO we might refactor this again to support `read`
findCellAndWrite ix a (Stream offset segment next) = do
  let !segIx = ix - offset
  -- TODO assert segIx is >= 0
  if segIx < sEGMENT_LENGTH
      -- assigned cell is within this segment
      then do 
  -- TODO we might want the winner between reader/writer of first cell to handle setting up next segment, so returbn two Bools here?
        wrote <- writeCell segment segIx a
  --TODO MAYBE RETURN BOOL AND MOVE THIS LOGIC INTO writeChan
        if wrote
          then when (segIx == 0) $ do -- NOTE [1]  -- TODO Test that on the very first write this is true
            -- optimistically try to set up next segment:
            void $ waitThenAdvanceStream next offset segSource 0

          -- retry if cell reader seen to have aborted:
          else writeChan c a
      -- check next segment, maybe creating it:
      else waitThenAdvanceStream next offset segSource (20*segIx) -- NOTE [2]
            >>= findCellAndWrite
  -- [1] the writer to the first cell of a new segment is tasked (somewhat
  -- arbitrarily) with trying to pre-allocate the *next* segment hopefully 
  -- ahead of any readers or writers who might need it. This will race with
  -- any reader or writer that tries to read the next segment and finds it's
  -- empty; when this wins (most of the time) we avoid a throughput hit.
  --
  -- [2] All readers or writers needing to work with a not-yet-created segment
  -- race to create it, but those past index 0 have progressively long waits; 20
  -- is chosen as 20 readIORefs should be more than enough time for writer/reader
  -- 0 to add the new segment (if it's not descheduled).


-- TODO tighten this up
{-# INLINE waitThenAdvanceStream #-}
waitThenAdvanceStream nextSegRef prevOffset segSource = go where
  go !wait = do
    tk <- readForCAS nextSegRef
    case peekTicket tk of
         NoSegment -> 
            if wait > 0  -- TODO GET THIS COMPILED AWAY WHEN CALLED w/ 0 constant
                then go (wait - 1)
                else do 
                  -- create a potential next segment and try to insert it:
                  let nextOffset = prevOffset + sEGMENT_LENGTH
                  potentialStrNext <- Stream nextOffset <$> segSource <*> newIORef NoSegment
                  (_,tkDone) <- casIORef nextSegRef tk (Next potentialStrNext)
                  -- that may have failed; regardless next segment is here:
                  case peekTicket tkDone of
                    NextSegment strNext -> return strNext
                    _ -> error "Impossible! This should only have been NextSegment"
         NextSegment strNext -> return strNext
           -- TODO assert that (segIx - sEGMENT_LENGTH ==  ix - offset strNext)


-- copying a template array is much faster than creating a new one; in fact we
-- need this in order to scale, since as cores increase we don't have enough
-- "runway" and can't allocate fast enough:
newSegmentSource :: IO (IO (StreamSegment a))
{-# INLINE newSegmentSource #-}
newSegmentSource = do
    arr <- newArray sEGMENT_LENGTH Empty
    return (cloneMutableArray arr 0 sEGMENT_LENGTH)

readChan :: OutChan a -> IO a
{-# INLINABLE readChan #-}
readChan c = mask_ $ do  -- NOTE [1]
  -- [1] we want our queue to behave like Chan in that (reasoning in terms of
  -- linearizability) the first reader waiting in line for an element may lose
  -- that element for good when an async exception raised (when the read and
  -- write overlap); but an async exception raised in a reader "blocked in line"
  -- (i.e. not at the head of the list) should never result in a lost message.

-- ----------
-- Each cell in a segment is assigned at most one reader and one writer
--
-- When all readers disappear and writers continue, we'll have at most one
-- segment's worth of garbage that can't be collected at a time.
--
-- TODO readers blocked indefinitely...?
-- ----------

-- Once we have the stream segment to which we've been assigned, write to our
-- assigned index:
writeCell :: StreamSegment a -> Int -> a -> IO Bool
{-# INLINE writeCell #-}
writeCell arr ix a = do
    cellTkt <- readArrayElem arr ix
    case peekTicket cellTkt of
         Empty -> do (success,cell) <- casArrayElem arr ix cellTkt a
                     if success 
                         then return True
                         else handleNotEmpty (peekTicket cell)
         cell -> handleNotEmpty cell

  where handleNotEmpty (Blocking v)   = putMVar v a >> return True
        -- a reader was killed (async exception) such that if `a` disappeared
        -- this might be observed to differ from the semantics of Chan. Retry:
        handleNotEmpty BlockedAborted = return False
        handleNotEmpty _ 
            = error "Impossible! Expected Blocking or BlockedAborted"

-- ...and likewise for the reader. Each index of each segment has at most one
-- reader and one writer assigned by the atomic counters.
readCell :: StreamSegment a -> Int -> IO a
{-# INLINE readCell #-}
readCell arr ix = do
    cellTkt <- readArrayElem arr ix
    case peekTicket cellTkt of
         Written a -> return a
         Empty -> do
            v <- newEmptyMVar
            (success,elseWrittenCell) <- casArrayElem arr ix cellTkt (Blocking v)
            if success 
                -- Block, waiting for the future writer.
                then takeMVar v `onException` (
                       -- re. writeArray race: if read overlaps with write,
                       -- this is undefined anyway:
                       writeArray arr ix BlockedAborted )
                -- In the meantime a writer has written. Good!
                else case peekTicket elseWrittenCell of
                          Written a -> return a
                          _ -> error "Impossible! Expecting Written"

         _ -> error "Impossible! Only expecting Empty or Written"


{- TESTS SKETCH
 - Test these assumptions:
     1) If a CAS fails in thread 1 then another CAS (in thread 2, say) succeeded
     2) In the case that thread 1's CAS failed, the ticket returned with (False,tk) will contain that newly-written value from thread 2
 -}

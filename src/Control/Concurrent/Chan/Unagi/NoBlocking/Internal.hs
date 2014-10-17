{-# LANGUAGE BangPatterns , DeriveDataTypeable, CPP #-}
module Control.Concurrent.Chan.Unagi.NoBlocking.Internal
#ifdef NOT_x86
    {-# WARNING "This library is unlikely to perform well on architectures without a fetch-and-add instruction" #-}
#endif
    (sEGMENT_LENGTH
    , InChan(..), OutChan(..), ChanEnd(..), StreamSegment, Cell, Stream(..)
    , NextSegment(..), StreamHead(..)
    , newChanStarting, writeChan, readChan, Element(..)
    , dupChan
    )
    where

-- Forked from src/Control/Concurrent/Chan/Unagi/Internal.hs at 065cd68010
--
-- The implementation here is Control.Concurrent.Chan.Unagi with the blocking
-- read mechanics removed, the required CAS rendevouz replaced with
-- writeArray/readArray, and MPSC/SPMC/SPSC variants that eliminate streamHead
-- updates and atomic operations on any 'S' sides.

import Data.IORef
import Control.Exception
import Control.Monad.Primitive(RealWorld)
import Data.Atomics.Counter.Fat
import Data.Atomics
import qualified Data.Primitive as P
import Control.Monad
import Control.Applicative
import Data.Bits
import Data.Typeable(Typeable)

import Control.Concurrent.Chan.Unagi.Constants


-- | The write end of a channel created with 'newChan'.
newtype InChan a = InChan (ChanEnd a)
    deriving (Typeable,Eq)

-- | The read end of a channel created with 'newChan'.
newtype OutChan a = OutChan (ChanEnd a)
    deriving (Typeable,Eq)

instance Eq (ChanEnd a) where
     (ChanEnd _ _ headA) == (ChanEnd _ _ headB)
        = headA == headB

-- TODO POTENTIAL CPP FLAGS (or functions)
--   - Strict element (or lazy? maybe also expose a writeChan' when relevant?)
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
    deriving Typeable

data StreamHead a = StreamHead !Int !(Stream a)

--TODO later see if we get a benefit from the small array primops in 7.10,
--     which omit card-marking overhead and might have faster clone.
type StreamSegment a = P.MutableArray RealWorld (Cell a)

-- TRANSITIONS and POSSIBLE VALUES:
--   During Read:
--     Nothing
--     Just a
--   During Write:
--     Nothing   -> Just a
type Cell a = Maybe a

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

data NextSegment a = NoSegment | Next !(Stream a)

-- we expose `startingCellOffset` for debugging correct behavior with overflow:
newChanStarting :: Int -> IO (InChan a, OutChan a)
{-# INLINE newChanStarting #-}
newChanStarting !startingCellOffset = do
    segSource <- newSegmentSource
    stream <- Stream <$> segSource 
                     <*> newIORef NoSegment
    let end = ChanEnd segSource 
                  <$> newCounter (startingCellOffset - 1)
                  <*> newIORef (StreamHead startingCellOffset stream)
    liftA2 (,) (InChan <$> end) (OutChan <$> end)

-- | Duplicate a chan: the returned @OutChan@ begins empty, but data written to
-- the argument @InChan@ from then on will be available from both the original
-- @OutChan@ and the one returned here, creating a kind of broadcast channel.
dupChan :: InChan a -> IO (OutChan a)
{-# INLINE dupChan #-}
dupChan (InChan (ChanEnd segSource counter streamHead)) = do
    hLoc <- readIORef streamHead
    loadLoadBarrier  -- NOTE [1]
    wCount <- readCounter counter
    
    counter' <- newCounter wCount 
    streamHead' <- newIORef hLoc
    return $ OutChan (ChanEnd segSource counter' streamHead')
  -- [1] We must read the streamHead before inspecting the counter; otherwise,
  -- as writers write, the stream head pointer may advance past the cell
  -- indicated by wCount.

-- | Write a value to the channel.
writeChan :: InChan a -> a -> IO ()
{-# INLINE writeChan #-}
writeChan (InChan ce@(ChanEnd segSource _ _)) = \a-> mask_ $ do 
    (segIx, (Stream seg next)) <- moveToNextCell ce
    P.writeArray seg segIx (Just a)
    -- try to pre-allocate next segment; NOTE [1]
    when (segIx == 0) $ void $
      waitingAdvanceStream next segSource 0
  -- [1] the writer which arrives first to the first cell of a new segment is
  -- tasked (somewhat arbitrarily) with trying to pre-allocate the *next*
  -- segment hopefully ahead of any readers or writers who might need it. This
  -- will race with any reader *or* writer that tries to read the next segment
  -- and finds it's empty (see `waitingAdvanceStream`); when this wins
  -- (hopefully the vast majority of the time) we avoid a throughput hit.

-- TODO or 'Item'? And something shorter than 'peeklement'?

-- | An idempotent @IO@ action that returns a particular enqueued element when
-- and if it becomes available. Each @Element@ corresponds to a particular
-- enqueued element, i.e. a returned @Element@ always offers the only means to
-- access one particular enqueued item.
newtype Element a = Element { peekElement :: IO (Maybe a) }

-- | Read an element from the chan, returning an @'Element' a@ future which
-- returns an actual element, when available, via 'peekElement'.
--
-- /Note re. exceptions/: When an async exception is raised during a @readChan@ 
-- the message that the read would have returned is likely to be lost, just as
-- it would be when raised directly after this function returns.
readChan :: OutChan a -> IO (Element a)
{-# INLINE readChan #-}
readChan (OutChan ce) = do  -- NOTE 1
    (segIx, (Stream seg _)) <- moveToNextCell ce
    return $ Element $ P.readArray seg segIx
 -- [1] We don't need to mask exceptions here. We say that exceptions raised in
 -- readChan are linearizable as occuring just before we are to return with our
 -- element. Note that the two effects in moveToNextCell are to increment the
 -- counter (this is the point after which we lose the read), and set up any
 -- future segments required (all atomic operations).

-- TODO consider offering a readChanYield helper

-- TODO use moveToNextCell/waitingAdvanceStream from Unagi.hs, only we'd need
--      to parameterize those functions and types by 'Cell a' rather than 'a'.

-- increments counter, finds stream segment of corresponding cell (updating the
-- stream head pointer as needed), and returns the stream segment and relative
-- index of our cell.
moveToNextCell :: ChanEnd a -> IO (Int, Stream a)
{-# INLINE moveToNextCell #-}
moveToNextCell (ChanEnd segSource counter streamHead) = do
    (StreamHead offset0 str0) <- readIORef streamHead
    -- NOTE [3]
#ifdef NOT_x86 
    -- fetch-and-add is a full barrier on x86; otherwise we need to make sure
    -- the read above occurrs before our fetch-and-add:
    loadLoadBarrier
#endif
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
    when (segsAway > 0) $ do
        let !offsetN = 
              offset0 + (segsAway `unsafeShiftL` lOG_SEGMENT_LENGTH) --(segsAway*sEGMENT_LENGTH)
        writeIORef streamHead $ StreamHead offsetN str -- NOTE [2]
    return (segIx,str)
  -- [1] All readers or writers needing to work with a not-yet-created segment
  -- race to create it, but those past index 0 have progressively long waits; 20
  -- is chosen as 20 readIORefs should be more than enough time for writer/reader
  -- 0 to add the new segment (if it's not descheduled).
  --
  -- [2] advancing the stream head pointer on segIx == sEGMENT_LENGTH - 1 would
  -- be more correct, but this is simpler here. This may move the head pointer
  -- BACKWARDS if the thread was descheduled, but that's not a correctness
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


-- copying a template array with cloneMutableArray is much faster than creating
-- a new one; in fact it seems we need this in order to scale, since as cores
-- increase we don't have enough "runway" and can't allocate fast enough:
type SegSource a = IO (StreamSegment a)

newSegmentSource :: IO (SegSource a)
newSegmentSource = do
    arr <- P.newArray sEGMENT_LENGTH Nothing
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

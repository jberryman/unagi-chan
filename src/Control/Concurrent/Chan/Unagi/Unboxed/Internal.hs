{-# LANGUAGE BangPatterns , DeriveDataTypeable, CPP , ScopedTypeVariables #-}
module Control.Concurrent.Chan.Unagi.Unboxed.Internal
#ifdef NOT_x86
    {-# WARNING "This library is unlikely to perform well on architectures without a fetch-and-add instruction" #-}
#endif
    (sEGMENT_LENGTH
    , InChan(..), OutChan(..), ChanEnd(..), Cell, Stream(..), ElementArray(..), SignalIntArray
    , readElementArray, writeElementArray
    , NextSegment(..), StreamHead(..)
    , newChanStarting, writeChan, readChan, readChanOnException
    , dupChan
    )
    where

-- Forked from src/Control/Concurrent/Chan/Unagi/Internal.hs at 443465. See
-- that implementation for additional details and notes which we omit here.
--
-- Internals exposed for testing and for re-use in Unagi.NoBlocking.Unboxed
--
-- TODO 
--   - Look at how ByteString is implemented; maybe that approach with
--     ForeignPtr is better in some ways, or perhaps we can use their Internals?
--       - we can make IndexedMVar () and always write to ByteString
--       - Also 'vector' lib


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
import GHC.Exts(inline)
import Utilities

import Control.Concurrent.Chan.Unagi.Constants

-- | The write end of a channel created with 'newChan'.
newtype InChan a = InChan (ChanEnd a)
    deriving Typeable

-- | The read end of a channel created with 'newChan'.
newtype OutChan a = OutChan (ChanEnd a)
    deriving Typeable

instance Eq (InChan a) where
    (InChan (ChanEnd _ _ headA)) == (InChan (ChanEnd _ _ headB))
        = headA == headB
instance Eq (OutChan a) where
    (OutChan (ChanEnd _ _ headA)) == (OutChan (ChanEnd _ _ headB))
        = headA == headB

-- The magic value we initialize our ElementArray with. When we're dealing with
-- objects word-sized or smaller (for atomicity) the reader can return with its
-- element after only inspecting the ElementArray when the value it reads does
-- not equal the magic value.
type Magic a = a

-- InChan & OutChan are mostly identical, sharing a stream, but with
-- independent counters
data ChanEnd a = 
            -- the value we initialize our ElementArray with. We only need this
            -- in read here, but will use it on the write side in other
            -- implementations:
   ChanEnd  !(Magic a)
            -- Both Chan ends must start with the same counter value.
            !AtomicCounter 
            -- the stream head; this must never point to a segment whose offset
            -- is greater than the counter value
            !(IORef (StreamHead a))
    deriving Typeable

data StreamHead a = StreamHead !Int !(Stream a)


-- The array we actually store our Prim elements in
newtype ElementArray a = ElementArray (P.MutableByteArray RealWorld)
-- TODO 
--   - we could easily use 'vector' to support a wider array of primitive
--      elements here.
--       - and what about Storable?
--     see http://stackoverflow.com/q/4908880/176841

readElementArray :: (P.Prim a)=> ElementArray a -> Int -> IO a
{-# INLINE readElementArray #-}
readElementArray (ElementArray arr) i = P.readByteArray arr i

writeElementArray :: (P.Prim a)=> ElementArray a -> Int -> a -> IO ()
{-# INLINE writeElementArray #-}
writeElementArray (ElementArray arr) i a = P.writeByteArray arr i a

-- We CAS on this, using Ints to signal (see below)
type SignalIntArray = P.MutableByteArray RealWorld

-- TRANSITIONS and POSSIBLE VALUES:
--   During Read:
--     Empty   -> Blocking
--     Written
--     Blocking (only when dupChan used)
--   During Write:
--     Empty   -> Written 
--     Blocking
{-
data Cell a = Empty    -- 0
            | Written  -- 1
            | Blocking -- 2
-}
type Cell = Int
cellEmpty, cellWritten, cellBlocking :: Cell
cellEmpty    = 0
cellWritten  = 1
cellBlocking = 2


segSource :: forall a. (P.Prim a)=> IO (SignalIntArray, ElementArray a) --ScopedTypeVariables
{-# INLINE segSource #-}
segSource = do
    let eBytes = P.sizeOf (undefined :: a) `unsafeShiftL` lOG_SEGMENT_LENGTH
    -- A largish pinned array seems like it would be the best choice here.
    sigArr <- P.newAlignedPinnedByteArray 
                (P.sizeOf    cellEmpty `unsafeShiftL` lOG_SEGMENT_LENGTH) -- times sEGMENT_LENGTH
                (P.alignment cellEmpty)
    eArr <- P.newAlignedPinnedByteArray 
                eBytes
                (P.alignment (undefined :: a))
    P.setByteArray sigArr 0 sEGMENT_LENGTH cellEmpty
    P.fillByteArray eArr 0 eBytes 0xDA -- why not an anti-programming mvt?
    return (sigArr, ElementArray eArr)

-- NOTE: we tried combining the SignalIntArray and ElementArray into a single
-- bytearray in the unagi-unboxed-combined-bytearray branch but saw no
-- significant improvement.
data Stream a = 
    Stream !SignalIntArray
           !(ElementArray a)
           -- For coordinating blocking between reader/writer; NOTE [1]
           (IndexedMVar a) -- N.B. must remain non-strict for NoBlocking.Unboxed
           -- The next segment in the stream; NOTE [2] 
           !(IORef (NextSegment a))
  -- [1] An important property: we can switch out this implementation as long
  -- as it utilizes a fresh MVar for each reader/writer pair.
  --
  -- [2] new segments are allocated and put here as we go, with threads
  -- cooperating to allocate new segments:

data NextSegment a = NoSegment | Next !(Stream a)

-- we expose `startingCellOffset` for debugging correct behavior with overflow:
newChanStarting :: (P.Prim a)=> Int -> IO (InChan a, OutChan a)
{-# INLINE newChanStarting #-}
newChanStarting !startingCellOffset = do
    (sigArr0,eArr0@(ElementArray eArr0')) <- segSource
    magic <- P.readByteArray eArr0' 0
    stream <- Stream sigArr0 eArr0 <$> newIndexedMVar <*> newIORef NoSegment
    let end = ChanEnd magic
                  <$> newCounter (startingCellOffset - 1)
                  <*> newIORef (StreamHead startingCellOffset stream)
    liftA2 (,) (InChan <$> end) (OutChan <$> end)


-- | Duplicate a chan: the returned @OutChan@ begins empty, but data written to
-- the argument @InChan@ from then on will be available from both the original
-- @OutChan@ and the one returned here, creating a kind of broadcast channel.
dupChan :: InChan a -> IO (OutChan a)
{-# INLINE dupChan #-}
dupChan (InChan (ChanEnd magic counter streamHead)) = do
    hLoc <- readIORef streamHead
    loadLoadBarrier
    wCount <- readCounter counter
    OutChan <$> (ChanEnd magic <$> newCounter wCount <*> newIORef hLoc)


-- | Write a value to the channel.
writeChan :: (P.Prim a)=> InChan a -> a -> IO ()
{-# INLINE writeChan #-}
writeChan (InChan ce) = \a-> mask_ $ do 
    (segIx, (Stream sigArr eArr mvarIndexed next), maybeUpdateStreamHead) <- moveToNextCell ce
    maybeUpdateStreamHead
    -- NOTE!: must write element before signaling with CAS:
    writeElementArray eArr segIx a
    actuallyWas <- casByteArrayInt sigArr segIx cellEmpty cellWritten -- NOTE[1]
    -- try to pre-allocate next segment:
    when (segIx == 0) $ void $
      waitingAdvanceStream next 0
    case actuallyWas of
         -- CAS SUCCEEDED: --
         0 {- Empty -} -> return ()
         -- CAS FAILED: --
         2 {- Blocking -} -> putMVarIx mvarIndexed segIx a
         1 {- Written -} -> error "Nearly Impossible! Expected Blocking"
         _ -> error "Invalid signal seen in writeChan!"
  -- [1] casByteArrayInt provides the write barrier we need here to make sure
  -- GHC maintains our ordering such that the element is written before we
  -- signal its availability with the CAS to sigArr that follows. See [2] in
  -- readChanOnExceptionUnmasked:


readChanOnExceptionUnmasked :: (P.Prim a)=> (IO a -> IO a) -> OutChan a -> IO a
{-# INLINE readChanOnExceptionUnmasked #-}
readChanOnExceptionUnmasked h = \(OutChan ce)-> do
    (segIx, (Stream sigArr eArr mvarIndexed _), maybeUpdateStreamHead) <- moveToNextCell ce
    maybeUpdateStreamHead
    let readBlocking = inline h $ readMVarIx mvarIndexed segIx    -- NOTE [1]
        readElem = readElementArray eArr segIx
    -- optimistically try read w/out CAS
    sig <- P.readByteArray sigArr segIx
    case (sig :: Int) of
         1 {- Written -} -> loadLoadBarrier >> readElem -- NOTE [2]
         2 {- Blocking -} -> readBlocking
         _ -> assert (sig == cellEmpty) $ do
            -- casByteArrayInt is a full barrier:
            actuallyWas <- casByteArrayInt sigArr segIx cellEmpty cellBlocking
            case actuallyWas of
                 -- succeeded writing Empty; proceed with blocking
                 0 {- Empty -} -> readBlocking
                 -- else in the meantime, writer wrote
                 1 {- Written -} -> readElem
                 -- else in the meantime a dupChan reader read, blocking
                 2 {- Blocking -} -> readBlocking
                 _ -> error "Invalid signal seen in readChanOnExceptionUnmasked!"
  -- [1] we must use `readMVarIx` here to support `dupChan`. It's also
  -- important that the behavior of readMVarIx be identical to a readMVar on
  -- the same MVar.
  --
  -- [2] We must make sure that we do the readElementArray only after reading
  -- the sigArr. Even though there is causality here (we inspect 'sig' and then
  -- maybe do 'readElem') we can't be sure the generated code won't do
  -- something clever or our processor won't optimistically do the readElem
  -- anyway (in which case we might get a garbage value). See [1] in writeChan.


-- | Read an element from the chan, blocking if the chan is empty.
--
-- /Note re. exceptions/: When an async exception is raised during a @readChan@ 
-- the message that the read would have returned is likely to be lost, even when
-- the read is known to be blocked on an empty queue. If you need to handle
-- this scenario, you can use 'readChanOnException'.
readChan :: (P.Prim a)=> OutChan a -> IO a
{-# INLINE readChan #-}
readChan = readChanOnExceptionUnmasked id

-- | Like 'readChan' but allows recovery of the queue element which would have
-- been read, in the case that an async exception is raised during the read. To
-- be precise exceptions are raised, and the handler run, only when
-- @readChanOnException@ is blocking.
--
-- The second argument is a handler that takes a blocking IO action returning
-- the element, and performs some recovery action.  When the handler is called,
-- the passed @IO a@ is the only way to access the element.
readChanOnException :: (P.Prim a)=> OutChan a -> (IO a -> IO ()) -> IO a
{-# INLINE readChanOnException #-}
readChanOnException c h = mask_ $ 
    readChanOnExceptionUnmasked (\io-> io `onException` (h io)) c



-- increments counter, finds stream segment of corresponding cell (updating the
-- stream head pointer as needed), and returns the stream segment and relative
-- index of our cell.
moveToNextCell :: (P.Prim a)=> ChanEnd a -> IO (Int, Stream a, IO ())
{-# INLINE moveToNextCell #-}
moveToNextCell (ChanEnd magic counter streamHead) = do
    (StreamHead offset0 str0) <- readIORef streamHead
    ix <- incrCounter 1 counter
    let (segsAway, segIx) = assert ((ix - offset0) >= 0) $ 
                 divMod_sEGMENT_LENGTH $! (ix - offset0)
              -- (ix - offset0) `quotRem` sEGMENT_LENGTH
        {-# INLINE go #-}
        go 0 str = return str
        go !n (Stream _ _ _ next) =
            waitingAdvanceStream next (nEW_SEGMENT_WAIT*segIx)
              >>= go (n-1)
    str <- go segsAway str0
    -- We need to return this continuation here for NoBlocking.Unboxed, which
    -- needs to perform this action at different points in the reader and
    -- writer.
    let !maybeUpdateStreamHead = 
          when (segsAway > 0) $ do
            let !offsetN = 
                  offset0 + (segsAway `unsafeShiftL` lOG_SEGMENT_LENGTH) --(segsAway*sEGMENT_LENGTH)
            writeIORef streamHead $ StreamHead offsetN str
    return (segIx,str, maybeUpdateStreamHead)


-- thread-safely try to fill `nextSegRef` at the next offset with a new
-- segment, waiting some number of iterations (for other threads to handle it).
-- Returns nextSegRef's StreamSegment.
waitingAdvanceStream :: (P.Prim a)=> IORef (NextSegment a) -> Int -> IO (Stream a)
waitingAdvanceStream nextSegRef = go where
  go !wait = assert (wait >= 0) $ do
    tk <- readForCAS nextSegRef
    case peekTicket tk of
         NoSegment 
           | wait > 0 -> go (wait - 1)
             -- Create a potential next segment and try to insert it:
           | otherwise -> do 
               potentialStrNext <- uncurry Stream 
                                            <$> segSource 
                                            <*> newIndexedMVar
                                            <*> newIORef NoSegment
               (_,tkDone) <- casIORef nextSegRef tk (Next potentialStrNext)
               -- If that failed another thread succeeded (no false negatives)
               case peekTicket tkDone of
                 Next strNext -> return strNext
                 _ -> error "Impossible! This should only have been Next segment"
         Next strNext -> return strNext

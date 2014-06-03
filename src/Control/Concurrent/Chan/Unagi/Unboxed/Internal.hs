{-# LANGUAGE BangPatterns , DeriveDataTypeable, CPP , ScopedTypeVariables #-}
module Control.Concurrent.Chan.Unagi.Unboxed.Internal
#ifdef NOT_x86
    {-# WARNING "This library is unlikely to perform well on architectures without a fetch-and-add instruction" #-}
#endif
    (sEGMENT_LENGTH
    , InChan(..), OutChan(..), ChanEnd(..), Cell, Stream(..), ElementArray(..), SignalIntArray
    , NextSegment(..), StreamHead(..)
    , newChanStarting, writeChan, readChan, readChanOnException
    )
    where

-- Forked from src/Control/Concurrent/Chan/Unagi/Internal.hs at 443465. See
-- that implementation for additional details.
--
-- Internals exposed for testing.
--
-- TODO CHECK DIFFERENT WRITE PATTERNS FOR CACHE W/ BYTE ARRAYS
--
-- TODO Look at how ByteString is implemented; maybe that approach is better in
--      some ways, or perhaps we can use their Internals.

import Control.Concurrent.MVar
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
import GHC.Conc(getNumProcessors)

-- TODO WRT GARBAGE COLLECTION
--  This can lead to large amounts of memory use in theory:
--   1. overhead of pre-allocated arrays and fat counter
--   2. already-read elements in arrays not-yet GC'd
--   3. array and element overhead from a writer/reader delayed (many
--      intermediate chunks held)
--
-- Ideas:
--   - hold weak reference to previous segment, and when we move the head
--     pointer, update `next` to point to this new segment (if it is still
--     around?)


-- Number of reads on which to spin for new segment creation.
-- Back-of-envelope (time_to_create_new_segment / time_for_read_IOref) + margin.
-- See usage site.
nEW_SEGMENT_WAIT :: Int
nEW_SEGMENT_WAIT = round (((14.6::Float) + 0.3*fromIntegral sEGMENT_LENGTH) / 3.7) + 10

newtype InChan a = InChan (ChanEnd a)
    deriving Typeable
newtype OutChan a = OutChan (ChanEnd a)
    deriving Typeable

instance Eq (InChan a) where
    (InChan (ChanEnd _ headA _)) == (InChan (ChanEnd _ headB _))
        = headA == headB
instance Eq (OutChan a) where
    (OutChan (ChanEnd _ headA _)) == (OutChan (ChanEnd _ headB _))
        = headA == headB

-- TODO POTENTIAL CPP FLAGS (or functions)
--   - Strict element (or lazy? maybe also reveal a writeChan' when relevant?)
--   - sEGMENT_LENGTH
--   - reads that clear the element immediately (or export as a special function?)

-- InChan & OutChan are mostly identical, sharing a stream, but with
-- independent counters
data ChanEnd a = 
            -- Both Chan ends must start with the same counter value.
    ChanEnd !AtomicCounter 
            -- the stream head; this must never point to a segment whose offset
            -- is greater than the counter value
            !(IORef (StreamHead a))
            -- a simple blocking queue
            !(BlockingChan a)
    deriving Typeable

data StreamHead a = StreamHead !Int !(Stream a)


-- The array we actually store our Prim elements in
newtype ElementArray a = ElementArray (P.MutableByteArray RealWorld)
-- TODO we could easily use 'vector' to support a wider array of primitive
--      elements here.

readElementArray :: (P.Prim a)=> ElementArray a -> Int -> IO a
readElementArray (ElementArray arr) = P.readByteArray arr

writeElementArray :: (P.Prim a)=> ElementArray a -> Int -> a -> IO ()
writeElementArray (ElementArray arr) = P.writeByteArray arr

-- We CAS on this, using Ints to signal (see below)
type SignalIntArray = P.MutableByteArray RealWorld

-- TRANSITIONS and POSSIBLE VALUES:
--   During Read:
--     Empty   -> Blocking
--     Written
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
cellEmpty = 0
cellWritten = 1
cellBlocking = 2


-- -------------------------------- TODO TEST
-- Factor out the blocking stuff in case we want to replace this later.
data BlockingChan a = BlockingChan !Int !(P.Array (MVar a))

newBlockingChan :: IO (BlockingChan a)
newBlockingChan = do
    -- somewhat arbitrary:
    !size <- nextHighestPowerOfTwo . (*2) <$> getNumProcessors
    let !sizeMn1 = size - 1
    mArr <- P.newArray size undefined
    forM_ [0..sizeMn1] $ \i->
        newEmptyMVar >>= P.writeArray mArr i

    -- in write/readBlocking we're passing segIx, the counter value modulo
    -- segment size, so we'd like this to hold. But not correctness issue.
    assert (size <= sEGMENT_LENGTH && sEGMENT_LENGTH `rem` size == 0) $
        BlockingChan sizeMn1 <$> P.unsafeFreezeArray mArr

-- We pass the Int from the atomic counter to be taken modulo the size of the
-- array to determine specific bucket to put/take from:
writeBlocking :: BlockingChan a -> Int -> a -> IO ()
{-# INLINE writeBlocking #-}
writeBlocking (BlockingChan sizeMn1 arr) n = 
    assert (n >= 0) $
      putMVar (P.indexArray arr (n .&. sizeMn1))

readBlocking :: BlockingChan a -> Int -> IO a
{-# INLINE readBlocking #-}
readBlocking (BlockingChan sizeMn1 arr) n =
    assert (n >= 0) $
      takeMVar (P.indexArray arr (n .&. sizeMn1))

-- Not particularly fast; if needs moar fast see
--   http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
nextHighestPowerOfTwo :: Int -> Int
nextHighestPowerOfTwo 0 = 1
nextHighestPowerOfTwo n =  
    2 ^ (ceiling (logBase 2 $ fromIntegral $ abs n :: Float) :: Int)
-- --------------------------------


-- TODO TEST
segSource :: forall a. (P.Prim a)=> IO (SignalIntArray, ElementArray a) --ScopedTypeVariables
{-# INLINE segSource #-}
segSource = do
    -- A largish pinned array seems like it would be the best choice here. TODO test unpinned
    sigArr <- P.newPinnedByteArray 
                (P.sizeOf cellEmpty `unsafeShiftL` pOW) -- * sEGMENT_LENGTH
    eArr <- P.newPinnedByteArray 
                (P.sizeOf (undefined :: a) `unsafeShiftL` pOW)
    P.setByteArray sigArr 0 sEGMENT_LENGTH cellEmpty
    return (sigArr, ElementArray eArr)


-- Constant for now: back-of-envelope considerations:
--   - making most of constant factor for cloning array of *any* size
--   - make most of overheads of moving to the next segment, etc.
--   - provide enough runway for creating next segment when 32 simultaneous writers 
--   - the larger this the larger one-time cost for the lucky writer
--   - don't fragment the pinned heap section
--
sEGMENT_LENGTH :: Int
{-# INLINE sEGMENT_LENGTH #-}
sEGMENT_LENGTH = 1024 -- NOTE: THIS REMAIN A POWER OF 2!

-- NOTE In general we'll have two segments allocated at any given time in
-- addition to the segment template, so in the worst case, when the program
-- exits we will have allocated ~ 3 segments extra memory than was actually
-- required.

data Stream a = 
    Stream !SignalIntArray
           !(ElementArray a)
           -- The next segment in the stream; new segments are allocated and
           -- put here as we go, with threads cooperating to allocate new
           -- segments:
           !(IORef (NextSegment a))

data NextSegment a = NoSegment | Next !(Stream a)

-- we expose `startingCellOffset` for debugging correct behavior with overflow:
newChanStarting :: (P.Prim a)=> Int -> IO (InChan a, OutChan a)
{-# INLINE newChanStarting #-}
newChanStarting !startingCellOffset = do
    stream <- uncurry Stream <$> segSource <*> newIORef NoSegment
    blockingChan <- newBlockingChan
    let end = ChanEnd
                  <$> newCounter (startingCellOffset - 1)
                  <*> newIORef (StreamHead startingCellOffset stream)
                  <*> pure blockingChan
    liftA2 (,) (InChan <$> end) (OutChan <$> end)


writeChan :: (P.Prim a)=> InChan a -> a -> IO ()
{-# INLINE writeChan #-}
writeChan (InChan ce) = \a-> mask_ $ do 
    (segIx, (Stream sigArr eArr next)) <- moveToNextCell ce

    -- NOTE!: must write element before signaling with CAS:
    writeElementArray eArr segIx a
#ifdef NOT_x86 
    -- TODO Should we include this for correctness sake? Will GHC ever move a write ahead of a CAS?
    -- CAS provides a full barrier on x86; otherwise we need to make sure the
    -- read above occurrs before our fetch-and-add:
    writeBarrier
#endif
    actuallyWas <- casByteArrayInt sigArr segIx cellEmpty cellWritten

    -- try to pre-allocate next segment; NOTE [1]
    when (segIx == 0) $ void $
      waitingAdvanceStream next 0
    case actuallyWas of
         0 {- Empty -} -> return ()
         2 {- Blocking -} -> 
            case ce of
                 (ChanEnd _ _ b) -> writeBlocking b segIx a
         1 -> error "Nearly Impossible! Expected Blocking"
         _ -> error "Invalid signal seen in writeChan!"
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
readChanOnExceptionUnmasked :: (P.Prim a)=> (IO a -> IO a) -> OutChan a -> IO a
{-# INLINE readChanOnExceptionUnmasked #-}
readChanOnExceptionUnmasked h = \(OutChan ce)-> do
    (segIx, (Stream sigArr eArr _)) <- moveToNextCell ce
    -- NOTE!: must CAS on signal before reading element. No barrier necessary.
    actuallyWas <- casByteArrayInt sigArr segIx cellEmpty cellBlocking
    case actuallyWas of
         -- succeeded writing Empty; proceed with blocking
         0 {- Empty -} -> 
            case ce of
                 (ChanEnd _ _ b) -> 
                    inline h $ readBlocking b segIx
         1 {- Written -} -> readElementArray eArr segIx
         2 {- Blocking -} -> error "Impossible! Only expecting Empty or Written"
         _ -> error "Invalid signal seen in readChanOnExceptionUnmasked!"


-- | Read an element from the chan, blocking if the chan is empty.
--
-- /Note re. exceptions/: When an async exception is raised during a @readChan@ 
-- the message that the read would have returned is likely to be lost, even when
-- the read is known to be blocked on an empty queue. If you need to handle
-- this scenario, you can use 'readChanOnException'.
readChan :: (P.Prim a)=> OutChan a -> IO a
{-# INLINE readChan #-}
readChan = readChanOnExceptionUnmasked id
          --TODO get id to disappear when inlined

-- | Like 'readChan' but allows recovery of the queue element which would have
-- been read, in the case that an async exception is raised during the read. To
-- be precise exceptions are raised, and the handler run, only when
-- @readChanOnException@ is blocking on an empty queue.
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
moveToNextCell :: (P.Prim a)=> ChanEnd a -> IO (Int, Stream a)
{-# INLINE moveToNextCell #-}
moveToNextCell (ChanEnd counter streamHead _) = do
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
        go !n (Stream _ _ next) =
            waitingAdvanceStream next (nEW_SEGMENT_WAIT*segIx) -- NOTE [1]
              >>= go (n-1)
    str <- go segsAway str0
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
                                            <*> newIORef NoSegment
               (_,tkDone) <- casIORef nextSegRef tk (Next potentialStrNext)
               -- If that failed another thread succeeded (no false negatives)
               case peekTicket tkDone of
                 Next strNext -> return strNext
                 _ -> error "Impossible! This should only have been Next segment"
         Next strNext -> return strNext


pOW, sEGMENT_LENGTH_MN_1 :: Int
pOW = round $ logBase (2::Float) $ fromIntegral sEGMENT_LENGTH -- or bit shifts in loop
sEGMENT_LENGTH_MN_1 = sEGMENT_LENGTH - 1

divMod_sEGMENT_LENGTH :: Int -> (Int,Int)
{-# INLINE divMod_sEGMENT_LENGTH #-}
divMod_sEGMENT_LENGTH n = let d = n `unsafeShiftR` pOW
                              m = n .&. sEGMENT_LENGTH_MN_1
                           in d `seq` m `seq` (d,m)

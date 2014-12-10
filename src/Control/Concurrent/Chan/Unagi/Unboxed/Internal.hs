{-# LANGUAGE BangPatterns , DeriveDataTypeable, CPP , ScopedTypeVariables #-}
module Control.Concurrent.Chan.Unagi.Unboxed.Internal
#ifdef NOT_x86
    {-# WARNING "This library is unlikely to perform well on architectures without a fetch-and-add instruction" #-}
#endif
    (sEGMENT_LENGTH
    , UnagiPrim(..)
    , InChan(..), OutChan(..), ChanEnd(..), Cell, Stream(..), ElementArray(..), SignalIntArray
    , readElementArray, writeElementArray
    , NextSegment(..), StreamHead(..), segSource
    , newChanStarting, writeChan, readChan, readChanOnException
    , dupChan
    )
    where

-- Forked from src/Control/Concurrent/Chan/Unagi/Internal.hs at 443465. See
-- that implementation for additional details and notes which we omit here.
--
-- Internals exposed for testing and for re-use in Unagi.NoBlocking.Unboxed
--
-- TODO integration w/ ByteString
--       - we'd need to make IndexedMVar () and always write to ByteString
--       - we'd need to switch to Storable probably.
--       - exporting slices of elements as ByteString 
--         - lazy bytestring would be easiest because of segment boundaries
--         - might be tricky to do blocking checking efficiently
--       - writing bytearrays
--         - fast with memcpy
--
-- TODO MAYBE another variation:
--    Either with a single reader, or a counter that tracks readers as they
--    exit a segment (so that we know when can be manually 'free'd), allowing
--    use of unmanaged memory, and:
--         - creatable with calloc
--         - replace IORef with an unboxed 1-element array holding Addr of MutableByteArray? or something...
--         - nullPtr can be used to get us references of (Maybe a)
--            > IORef (StreamHead                                 )
--            >       Int + (Stream                               )
--            >             arr + arr + IndexedMvar + Maybe Stream
--           - We would need to move IndexedMvar into streamhead
--         - No CAS for Ptr/ForeignPtr but we can probably extract the mutablebytearray for CAS
--            Data.Primitive.ByteArray.mutableByteArrayContents ~> Addr
--             , and ForeignPtr holds an Addr# + MutableByteArray internally...
--             , use GHC.ForeignPtr and wrap MutableByteArray in PlainPtr and off to races
--
-- TODO post allocation tests:
--       + move the copying code into no-blocking-variant-unboxed-2-copyMutableByteArray
--       + play with NOINLINE etc on all slow path code. Look at conditionals and case
--         + also consider not unboxing (and moving to back) data involved in that slow path
--         NO BENEFIT
--       + time using Addr for writes and reads
--         NO DIFFERENCE
--       + IN MULTI: test waiting longer to pre-allocate (so it stays in cache?)
--         NO CLEAR DIFFERENCE, even when I remove pre-allocation altogether
--       + try keeping around IndexedMvar
--         NO DIFFERENCE EVEN IN MOCKUP
--       + test replacing casByteArray with fetchAddByteArray (that CAS is a very large chunk of latency)
--         NOPE. JUST A LITTLE SLOWER IN write all test
--         + Use a small sigArr (like a bitmap) 
--           NOT REALLY VISIBLE AMORTIZED IN SINGLE BENCHMARK, BUT OBVIOUSLY GOOD FOR ALLOCATOR THREAD
--           - maybe even do something clever with fetch-and-add rather than CAS,
--              with different values being added to twiddle different parts of bit range
--           - for no-blocking variant, we could make size == smallest atomic write size
--              and that would give us a 4x (or 8x for 64-bit) smaller array if
--              we wanted to re-use that sizing everywhere
--       - use ghc-events-analyze for god's sake!:  http://www.well-typed.com/blog/86/
--
-- TODO GHC 7.10 or someday:
--       - use segment length of e.g. 1022 to account for MutableByteArray
--         header, then align to cache line (note: we don't really need to use
--         div/mod here; just subtraction) This could be done in all
--         implementations. (boxed arrays are: 3 + n/128 + n words?? Who knows...)
--       - calloc for mutableByteArray, when/if available
--       - non-temporal writes that bypass the cache? See: http://lwn.net/Articles/255364/
--       - SIMD stuff for batch writing, or zeroing, etc. etc
--
-- TODO TESTS:
--   - Magic value equality for all fields of array, with stored magic value, for many different Prim a
--   - All tests with different Prim a (both > and < sizeOf Word)
--   - word-overlapping atomic thread-safe writes of size < 1 word (see other Unboxed)
--       - 4 threads: incrementing Int8 individual bytes of a 4-byte arr, minBound -> maxBound
--       - same for Int16
--
-- TODO HERE:
--   - fix tests
--   - update CHANGELOG w/r/t unboxed changes
--   - add new tests needed
--   - continue with NoBlocking.Unboxed


import Data.IORef
import Control.Exception
import Control.Monad.Primitive(RealWorld)
import Data.Atomics.Counter.Fat
import Data.Atomics
import qualified Data.Primitive as P
import Control.Monad
import Control.Applicative
import Data.Bits
import GHC.Exts(inline)
-- For instances:
import Data.Typeable(Typeable)
import Data.Int(Int8,Int16,Int32,Int64)
import Data.Word(Word,Word8,Word16,Word32,Word64)

import Utilities
import Control.Concurrent.Chan.Unagi.Constants

-- | The write end of a channel created with 'newChan'.
newtype InChan a = InChan (ChanEnd a)
    deriving Typeable

-- | The read end of a channel created with 'newChan'.
newtype OutChan a = OutChan (ChanEnd a)
    deriving Typeable

instance Eq (InChan a) where
    (InChan (ChanEnd _ headA)) == (InChan (ChanEnd _ headB))
        = headA == headB
instance Eq (OutChan a) where
    (OutChan (ChanEnd _ headA)) == (OutChan (ChanEnd _ headB))
        = headA == headB


-- InChan & OutChan are mostly identical, sharing a stream, but with
-- independent counters
data ChanEnd a = 
   ChanEnd  -- Both Chan ends must start with the same counter value.
            !AtomicCounter 
            -- the stream head; this must never point to a segment whose offset
            -- is greater than the counter value
            !(IORef (StreamHead a))
    deriving Typeable

data StreamHead a = StreamHead !Int !(Stream a)


-- The array we actually store our Prim elements in
newtype ElementArray a = ElementArray (P.MutableByteArray RealWorld)

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


-- NOTE: attempts to make allocation and initialization faster via copying, or
-- other tricks failed; although a calloc was about 2x faster (but that was for
-- unmanaged memory)
segSource :: forall a. (UnagiPrim a)=> IO (SignalIntArray, ElementArray a) --ScopedTypeVariables
{-# INLINE segSource #-}
segSource = do
    -- A largish pinned array seems like it would be the best choice here.
    sigArr <- P.newAlignedPinnedByteArray 
                (P.sizeOf    cellEmpty `unsafeShiftL` lOG_SEGMENT_LENGTH) -- times sEGMENT_LENGTH
                (P.alignment cellEmpty)
    -- NOTE: we need these to be aligned to (some multiple of) Word boundaries
    -- for magic trick to be correct, and for assumptions about atomicity of
    -- loads/stores to hold!
    eArr <- P.newAlignedPinnedByteArray 
                (P.sizeOf (undefined :: a) `unsafeShiftL` lOG_SEGMENT_LENGTH)
                (P.alignment (undefined :: a))
    P.setByteArray sigArr 0 sEGMENT_LENGTH cellEmpty
    -- If no atomicUnicorn then we always check in at sigArr, so no need to
    -- initialize eArr:
    maybe (return ()) 
        (P.setByteArray eArr 0 sEGMENT_LENGTH) (atomicUnicorn :: Maybe a)
    return (sigArr, ElementArray eArr)
    -- NOTE: We always CAS this into place which provides write barrier, such
    -- that arrays are fully initialized before they can be read. No
    -- corresponding barrier is needed in waitingAdvanceStream.


-- | Our class of types supporting primitive array operations
class (P.Prim a, Eq a)=> UnagiPrim a where
    -- | When the read and write operations of the underlying @Prim@ instances
    -- on aligned memory are atomic, this may be set to @Just x@ where @x@ is
    -- some rare (i.e.  unlikely to occur frequently in your data) magic value;
    -- this might help speed up some @UnagiPrim@ operations.
    --
    -- Where those 'Prim' instance operations are not atomic, this *must* be
    -- set to @Nothing@.
    atomicUnicorn :: Maybe a
    atomicUnicorn = Nothing


-- These ought all to be atomic for 32-bit or 64-bit systems:
instance UnagiPrim Char	where
    atomicUnicorn = Just '\1010101'
instance UnagiPrim Float where
    atomicUnicorn = Just 0xDADADA
instance UnagiPrim Int where
    atomicUnicorn = Just 0xDADADA
instance UnagiPrim Int8	where
    atomicUnicorn = Just 113
instance UnagiPrim Int16 where
    atomicUnicorn = Just 0xDAD
instance UnagiPrim Int32 where
    atomicUnicorn = Just 0xDADADA
instance UnagiPrim Word	where
    atomicUnicorn = Just 0xDADADA
instance UnagiPrim Word8 where
    atomicUnicorn = Just 0xDA
instance UnagiPrim Word16 where
    atomicUnicorn = Just 0xDADA
instance UnagiPrim Word32 where
    atomicUnicorn = Just 0xDADADADA
instance UnagiPrim P.Addr where
    atomicUnicorn = Just P.nullAddr
-- These should conservatively be expected to be atomic only on 64-bit
-- machines:
instance UnagiPrim Int64 where
#ifdef IS_64_BIT
    atomicUnicorn = Just 0xDADADADADADA
#endif
instance UnagiPrim Word64 where
#ifdef IS_64_BIT
    atomicUnicorn = Just 0xDADADADADADA
#endif
instance UnagiPrim Double where
#ifdef IS_64_BIT
    atomicUnicorn = Just 0xDADADADADADA
#endif

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
newChanStarting :: UnagiPrim a=> Int -> IO (InChan a, OutChan a)
{-# INLINE newChanStarting #-}
newChanStarting !startingCellOffset = do
    (sigArr0,eArr0) <- segSource
    stream <- Stream sigArr0 eArr0 <$> newIndexedMVar <*> newIORef NoSegment
    let end = ChanEnd
                  <$> newCounter (startingCellOffset - 1)
                  <*> newIORef (StreamHead startingCellOffset stream)
    liftA2 (,) (InChan <$> end) (OutChan <$> end)


-- | Duplicate a chan: the returned @OutChan@ begins empty, but data written to
-- the argument @InChan@ from then on will be available from both the original
-- @OutChan@ and the one returned here, creating a kind of broadcast channel.
dupChan :: InChan a -> IO (OutChan a)
{-# INLINE dupChan #-}
dupChan (InChan (ChanEnd counter streamHead)) = do
    hLoc <- readIORef streamHead
    loadLoadBarrier
    wCount <- readCounter counter
    OutChan <$> (ChanEnd <$> newCounter wCount <*> newIORef hLoc)


-- | Write a value to the channel.
writeChan :: UnagiPrim a=> InChan a -> a -> IO ()
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
  -- readChanOnExceptionUnmasked.


readChanOnExceptionUnmasked :: UnagiPrim a=> (IO a -> IO a) -> OutChan a -> IO a
{-# INLINE readChanOnExceptionUnmasked #-}
readChanOnExceptionUnmasked h = \(OutChan ce)-> do
    (segIx, (Stream sigArr eArr mvarIndexed _), maybeUpdateStreamHead) <- moveToNextCell ce
    maybeUpdateStreamHead
    let readBlocking = inline h $ readMVarIx mvarIndexed segIx    -- NOTE [1]
        readElem = readElementArray eArr segIx
        slowRead = do 
           -- Assume probably blocking (Note: casByteArrayInt is a full barrier)
           actuallyWas <- casByteArrayInt sigArr segIx cellEmpty cellBlocking -- NOTE [2]
           case actuallyWas of
                -- succeeded writing Empty; proceed with blocking
                0 {- Empty -} -> readBlocking
                -- else in the meantime, writer wrote
                1 {- Written -} -> readElem
                -- else in the meantime a dupChan reader read, blocking
                2 {- Blocking -} -> readBlocking
                _ -> error "Invalid signal seen in readChanOnExceptionUnmasked!"
    -- If we know writes of this element are atomic, we can determine if the
    -- element has been written, and possibly return it without consulting
    -- sigArr.
    case atomicUnicorn of
         Just magic -> do
            el <- readElem
            if (el /= magic) 
              -- We know `el` was atomically written:
              then return el
              else slowRead
         Nothing -> slowRead
  -- [1] we must use `readMVarIx` here to support `dupChan`. It's also
  -- important that the behavior of readMVarIx be identical to a readMVar on
  -- the same MVar.
  --
  -- [2] casByteArrayInt provides the loadLoadBarrier we need here. See [1] in
  -- writeChan.


-- | Read an element from the chan, blocking if the chan is empty.
--
-- /Note re. exceptions/: When an async exception is raised during a @readChan@ 
-- the message that the read would have returned is likely to be lost, even when
-- the read is known to be blocked on an empty queue. If you need to handle
-- this scenario, you can use 'readChanOnException'.
readChan :: UnagiPrim a=> OutChan a -> IO a
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
readChanOnException :: UnagiPrim a=> OutChan a -> (IO a -> IO ()) -> IO a
{-# INLINE readChanOnException #-}
readChanOnException c h = mask_ $ 
    readChanOnExceptionUnmasked (\io-> io `onException` (h io)) c



-- increments counter, finds stream segment of corresponding cell (updating the
-- stream head pointer as needed), and returns the stream segment and relative
-- index of our cell.
moveToNextCell :: UnagiPrim a=> ChanEnd a -> IO (Int, Stream a, IO ())
{-# INLINE moveToNextCell #-}
moveToNextCell (ChanEnd counter streamHead) = do
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
waitingAdvanceStream :: (UnagiPrim a)=> IORef (NextSegment a) -> Int -> IO (Stream a)
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

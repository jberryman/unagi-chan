{-# LANGUAGE BangPatterns , DeriveDataTypeable, CPP #-}
module Control.Concurrent.Chan.Unagi.NoBlocking.Internal
#ifdef NOT_x86
    {-# WARNING "This library is unlikely to perform well on architectures without a fetch-and-add instruction" #-}
#endif
    (sEGMENT_LENGTH
    , InChan(..), OutChan(..), ChanEnd(..), StreamSegment, Cell, Stream(..)
    , NextSegment(..), StreamHead(..)
    , newChanStarting, writeChan, tryReadChan, readChan, Element(..)
    , dupChan
    , streamChan
    , isActive
    )
    where

-- Forked from src/Control/Concurrent/Chan/Unagi/Internal.hs at 065cd68010
--
-- Some detailed NOTEs present in Control.Concurrent.Chan.Unagi have been
-- removed here although they still pertain. If you intend to work on this 
-- module, please be sure you're familiar with those concerns.
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

import Utilities(touchIORef)
import Control.Concurrent.Chan.Unagi.Constants
import qualified Control.Concurrent.Chan.Unagi.NoBlocking.Types as UT


-- | The write end of a channel created with 'newChan'.
data InChan a = InChan !(IORef Bool) -- Used for creating an OutChan in dupChan
                       !(ChanEnd a)
    deriving (Typeable,Eq)

-- | The read end of a channel created with 'newChan'.
data OutChan a = OutChan !(IORef Bool) -- Is corresponding InChan still alive?
                         !(ChanEnd a) 
    deriving (Typeable,Eq)

instance Eq (ChanEnd a) where
     (ChanEnd _ _ headA) == (ChanEnd _ _ headB)
        = headA == headB


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
    inEnd@(ChanEnd _ _ inHeadRef) <- end
    finalizee <- newIORef True
    void $ mkWeakIORef inHeadRef $ do -- NOTE [1]
        -- make sure the array writes of any final writeChans occur before the
        -- following writeIORef. See isActive [*]:
        writeBarrier
        writeIORef finalizee False
    (,) (InChan finalizee inEnd) <$> (OutChan finalizee <$> end)
 -- [1] We no longer get blocked indefinitely exception in readers when all
 -- writers disappear, so we use finalizers. See also NOTE 1 in 'writeChan' and
 -- implementation of 'isActive' below.

-- | An action that returns @False@ sometime after the chan no longer has any
-- writers.
--
-- After @False@ is returned, any 'peekElement' which returns @Nothing@ can be
-- considered to be dead. Note that in the blocking implementations a
-- @BlockedIndefinitelyOnMVar@ exception is raised, so this function is
-- unnecessary.
isActive :: OutChan a -> IO Bool
isActive (OutChan finalizee _) = do
    b <- readIORef finalizee
    -- make sure that any peekElement that follows is not moved ahead. See
    -- newChanStarting [*]:
    loadLoadBarrier
    return b

-- | Duplicate a chan: the returned @OutChan@ begins empty, but data written to
-- the argument @InChan@ from then on will be available from both the original
-- @OutChan@ and the one returned here, creating a kind of broadcast channel.
--
-- See also 'streamChan' for a faster alternative that might be appropriate.
dupChan :: InChan a -> IO (OutChan a)
{-# INLINE dupChan #-}
dupChan (InChan finalizee (ChanEnd segSource counter streamHead)) = do
    hLoc <- readIORef streamHead
    loadLoadBarrier
    wCount <- readCounter counter
    counter' <- newCounter wCount 
    streamHead' <- newIORef hLoc
    return $ OutChan finalizee $ ChanEnd segSource counter' streamHead'


-- | Write a value to the channel.
writeChan :: InChan a -> a -> IO ()
{-# INLINE writeChan #-}
writeChan (InChan _ ce@(ChanEnd segSource _ _)) = \a-> mask_ $ do 
    (segIx, (Stream seg next), maybeUpdateStreamHead) <- moveToNextCell ce
    P.writeArray seg segIx (Just a)
    maybeUpdateStreamHead  -- NOTE [1]
    -- try to pre-allocate next segment:
    when (segIx == 0) $ void $
      waitingAdvanceStream next segSource 0
 -- [1] We return the maybeUpdateStreamHead action from moveToNextCell rather
 -- than running it before returning, because we must ensure that the
 -- streamHead IORef is not GC'd (and its finalizer run) before the last
 -- element is written; else the user has no way of being sure that it has read
 -- the last element. See 'newChanStarting' and 'isActive'.


-- TODO or 'Item'? And something shorter than 'peeklement'?

-- | An idempotent @IO@ action that returns a particular enqueued element when
-- and if it becomes available. Each @Element@ corresponds to a particular
-- enqueued element, i.e. a returned @Element@ always offers the only means to
-- access one particular enqueued item.
newtype Element a = Element { peekElement :: IO (Maybe a) }


-- | Read an element from the chan, returning an @'Element' a@ future which
-- returns an actual element, when available, via 'peekElement'.
--
-- /Note re. exceptions/: When an async exception is raised during a @tryReadChan@ 
-- the message that the read would have returned is likely to be lost, just as
-- it would be when raised directly after this function returns.
tryReadChan :: OutChan a -> IO (Element a)
{-# INLINE tryReadChan #-}
tryReadChan (OutChan _ ce) = do  -- NOTE [1]
    (segIx, (Stream seg _), maybeUpdateStreamHead) <- moveToNextCell ce
    maybeUpdateStreamHead
    return $ Element $ P.readArray seg segIx
 -- [1] We don't need to mask exceptions here. We say that exceptions raised in
 -- tryReadChan are linearizable as occuring just before we are to return with our
 -- element. Note that the two effects in moveToNextCell are to increment the
 -- counter (this is the point after which we lose the read), and set up any
 -- future segments required (all atomic operations).


-- | @readChan io c@ returns the next element from @c@, calling 'tryReadChan'
-- and looping on the 'Element' returned, and calling @io@ at each iteration
-- when the element is not yet available. It throws 'BlockedIndefinitelyOnMVar'
-- when 'isActive' determines that a value will never be returned.
--
-- When used like @readChan 'yield'@ or @readChan ('threadDelay' 10)@ this is
-- the semantic equivalent to the blocking @readChan@ in the other
-- implementations.
readChan :: IO () -> OutChan a -> IO a
{-# INLINE readChan #-}
readChan io oc = tryReadChan oc >>= \el->
    let peekMaybe f = peekElement el >>= maybe f return 
        go = peekMaybe checkAndGo
        checkAndGo = do 
            b <- isActive oc
            if b then io >> go
                 -- Do a necessary final check of the element:
                 else peekMaybe $ throwIO BlockedIndefinitelyOnMVar
     in go


-- TODO making this stream-agnostic:
--   - as currently have it = ListT, but does this make sense? The "end of
--     stream" simply indicates no more elements currently
--      - does pipes provide functions that return continuation (so we can
--        retry) on end of stream?
--         - 'next' is useful for us
--   - We could also have an agnostic function that does 'yield' and only calls
--     mzero when isActive returns False.
--
--   - then add benchmarks with pipes/IOStreams
--
--
--     TODO: Three streaming variants?:
--       - a streamChan that blocks (need isActive to use an MVar) until writers exit, 
--          which returns agostic ListT equivalent
--       - an agnostic streaming version that only returns mzero when isActive returns False, else calling yield
--       (both of above could use same concrete type instance, with accessor 'readStream')
--       - existing version (with concrete type)
--
-- TODO a write-side equivalent:
--   - can be made streaming agnostic?
--   - NOTE: if we're only streaming in and out, then using multiple chans is
--   possible (e.g. 3W6R is equivalent to 3 1:2 streaming chans)
--
-- TODO possible extension to streamChan
--   - overload streamChan for Streams too.
--


-- | Produce the specified number of interleaved \"streams\" from a chan.
-- Consuming a 'UI.Stream' is much faster than calling 'tryReadChan', and
-- might be useful when an MPSC queue is needed, or when multiple consumers
-- should be load-balanced in a round-robin fashion. 
--
-- Usage example:
--
-- > do mapM_ ('writeChan' i) [1..9]
-- >    [str1, str2, str2] <- 'streamChan' 3 o
-- >    forkIO $ printStream str1   -- prints: 1,4,7
-- >    forkIO $ printStream str2   -- prints: 2,5,8
-- >    forkIO $ printStream str3   -- prints: 3,6,9
-- >  where 
-- >    printStream str = do
-- >      h <- 'tryReadStream' str
-- >      case h of
-- >        'Cons' a str' -> print a >> printStream str'
-- >        -- We know that all values were already written, so a Pending tells 
-- >        -- us we can exit; in other cases we might call 'yield' and then 
-- >        -- retry that same @'tryReadStream' str@:
-- >        'Pending' -> return ()
streamChan :: Int -> OutChan a -> IO [UT.Stream a]
{-# INLINE streamChan #-}
streamChan period (OutChan _ (ChanEnd segSource counter streamHead)) = do
    when (period < 1) $ error "Argument to streamChan must be > 0"

    (StreamHead offsetInitial strInitial) <- readIORef streamHead
    -- Make sure the read above occurs before our readCounter:
    loadLoadBarrier
    -- Linearizable as the first unread element; N.B. (+1):
    !ix0 <- (+1) <$> readCounter counter

    -- Adapted from moveToNextCell, given a stream segment location `str0` and
    -- its offset, `offset0`, this navigates to the UT.Stream segment holding `ix`
    -- and begins recursing in our UT.Stream wrappers
    let stream !offset0 str0 !ix = UT.Stream $ do
            -- Find our stream segment and relative index:
            let (segsAway, segIx) = assert ((ix - offset0) >= 0) $ 
                         divMod_sEGMENT_LENGTH $! (ix - offset0)
                      -- (ix - offset0) `quotRem` sEGMENT_LENGTH
                {-# INLINE go #-}
                go 0 str = return str
                go !n (Stream _ next) =
                    waitingAdvanceStream next segSource (nEW_SEGMENT_WAIT*segIx)
                      >>= go (n-1)
            -- the stream segment holding `ix`, and its calculated offset:
            str@(Stream seg _) <- go segsAway str0
            let !strOffset = offset0+(segsAway `unsafeShiftL` lOG_SEGMENT_LENGTH)  
            --                       (segsAway  *                 sEGMENT_LENGTH)
            mbA <- P.readArray seg segIx
            case mbA of
                 Nothing -> return UT.Pending
                 -- Navigate to next cell and return this cell's value
                 -- along with the wrapped action to read from the next
                 -- cell and possibly recurse.
                 Just a -> return $ UT.Cons a $ stream strOffset str (ix+period)

    return $ map (stream offsetInitial strInitial) $
     -- [ix0..(ix0+period-1)] -- WRONG (hint: overflow)!
        take period $ iterate (+1) ix0


-- TODO use moveToNextCell/waitingAdvanceStream from Unagi.hs, only we'd need
--      to parameterize those functions and types by 'Cell a' rather than 'a'.
--      And use the version of moveToNextCell here, which returns the update
--      continuation.
--       - and N.B. touchIORef in moveToNextCell!

-- increments counter, finds stream segment of corresponding cell (updating the
-- stream head pointer as needed), and returns the stream segment and relative
-- index of our cell.
moveToNextCell :: ChanEnd a -> IO (Int, Stream a, IO ())
{-# INLINE moveToNextCell #-}
moveToNextCell (ChanEnd segSource counter streamHead) = do
    (StreamHead offset0 str0) <- readIORef streamHead
    ix <- incrCounter 1 counter
    let (segsAway, segIx) = assert ((ix - offset0) >= 0) $ 
                 divMod_sEGMENT_LENGTH $! (ix - offset0)
              -- (ix - offset0) `quotRem` sEGMENT_LENGTH
        {-# INLINE go #-}
        go 0 str = return str
        go !n (Stream _ next) =
            waitingAdvanceStream next segSource (nEW_SEGMENT_WAIT*segIx)
              >>= go (n-1)
    str <- go segsAway str0
    let !maybeUpdateStreamHead = do
          when (segsAway > 0) $ do
            let !offsetN = 
                  offset0 + (segsAway `unsafeShiftL` lOG_SEGMENT_LENGTH) --(segsAway*sEGMENT_LENGTH)
            writeIORef streamHead $ StreamHead offsetN str
          touchIORef streamHead -- NOTE [1]
    return (segIx,str, maybeUpdateStreamHead)
  -- [1] This helps ensure that our (possibly last) use of streamHead occurs
  -- after our (possibly last) write, for correctness of 'isActive'.  See NOTE
  -- 1 of 'writeChan'


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

{-# LANGUAGE BangPatterns , DeriveDataTypeable, CPP #-}
module Control.Concurrent.Chan.Unagi.NoBlocking.Internal
#ifdef NOT_x86
    {-# WARNING "This library is unlikely to perform well on architectures without a fetch-and-add instruction" #-}
#endif
    (sEGMENT_LENGTH
    , InChan(..), OutChan(..), ChanEnd(..), StreamSegment, Cell, Stream(..)
    , NextSegment(..), StreamHead(..)
    , newChanStarting, writeChan, tryReadChan, readChan, UT.Element(..)
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
import Data.Atomics.Counter.Fat
import Data.Atomics
import qualified Data.Primitive as P
import Control.Monad
import Control.Applicative
import Data.Bits
import Data.Typeable(Typeable)

import Control.Concurrent.Chan.Unagi.Internal(
    newSegmentSource, moveToNextCell, waitingAdvanceStream,
    ChanEnd(..), StreamHead(..), StreamSegment, Stream(..), NextSegment(..))
import Control.Concurrent.Chan.Unagi.Constants
import qualified Control.Concurrent.Chan.Unagi.NoBlocking.Types as UT


-- | The write end of a channel created with 'newChan'.
data InChan a = InChan !(IORef Bool) -- Used for creating an OutChan in dupChan
                       !(ChanEnd (Cell a))
    deriving (Typeable,Eq)

-- | The read end of a channel created with 'newChan'.
data OutChan a = OutChan !(IORef Bool) -- Is corresponding InChan still alive?
                         !(ChanEnd (Cell a)) 
    deriving (Typeable,Eq)


-- TRANSITIONS and POSSIBLE VALUES:
--   During Read:
--     Nothing
--     Just a
--   During Write:
--     Nothing   -> Just a
type Cell a = Maybe a


-- we expose `startingCellOffset` for debugging correct behavior with overflow:
newChanStarting :: Int -> IO (InChan a, OutChan a)
{-# INLINE newChanStarting #-}
newChanStarting !startingCellOffset = do
    segSource <- newSegmentSource Nothing
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
-- After @False@ is returned, any 'UT.tryRead' which returns @Nothing@ can
-- be considered to be dead. Likewise for 'UT.tryReadNext'. Note that in the
-- blocking implementations a @BlockedIndefinitelyOnMVar@ exception is raised,
-- so this function is unnecessary.
isActive :: OutChan a -> IO Bool
isActive (OutChan finalizee _) = do
    b <- readIORef finalizee
    -- make sure that any tryRead that follows is not moved ahead. See
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




   -- NOTE: this might be better named "claimElement" or something, but we'll
   -- keep the name since it's the closest equivalent to a real "tryReadChan"
   -- we can get in this design:

-- | Returns immediately with an @'UT.Element' a@ future, which returns one
-- unique element when it becomes available via 'UT.tryRead'.
--
-- /Note re. exceptions/: When an async exception is raised during a @tryReadChan@ 
-- the message that the read would have returned is likely to be lost, just as
-- it would be when raised directly after this function returns.
tryReadChan :: OutChan a -> IO (UT.Element a)
{-# INLINE tryReadChan #-}
tryReadChan (OutChan _ ce) = do  -- NOTE [1]
    (segIx, (Stream seg _), maybeUpdateStreamHead) <- moveToNextCell ce
    maybeUpdateStreamHead
    return $ UT.Element $ P.readArray seg segIx
 -- [1] We don't need to mask exceptions here. We say that exceptions raised in
 -- tryReadChan are linearizable as occuring just before we are to return with our
 -- element. Note that the two effects in moveToNextCell are to increment the
 -- counter (this is the point after which we lose the read), and set up any
 -- future segments required (all atomic operations).


-- | @readChan io c@ returns the next element from @c@, calling 'tryReadChan'
-- and looping on the 'UT.Element' returned, and calling @io@ at each iteration
-- when the element is not yet available. It throws 'BlockedIndefinitelyOnMVar'
-- when 'isActive' determines that a value will never be returned.
--
-- When used like @readChan 'yield'@ or @readChan ('threadDelay' 10)@ this is
-- the semantic equivalent to the blocking @readChan@ in the other
-- implementations.
readChan :: IO () -> OutChan a -> IO a
{-# INLINE readChan #-}
readChan io oc = tryReadChan oc >>= \el->
    let peekMaybe f = UT.tryRead el >>= maybe f return 
        go = peekMaybe checkAndGo
        checkAndGo = do 
            b <- isActive oc
            if b then io >> go
                 -- Do a necessary final check of the element:
                 else peekMaybe $ throwIO BlockedIndefinitelyOnMVar
     in go


-- TODO a write-side equivalent:
--   - can be made streaming agnostic?
--   - NOTE: if we're only streaming in and out, then using multiple chans is
--   possible (e.g. 3:6  is equivalent to 3 sets of 1:2 streaming chans)
--
-- TODO MAYBE: overload `streamChan` for Streams too.


-- | Produce the specified number of interleaved \"streams\" from a chan.
-- Nextuming a 'UI.Stream' is much faster than calling 'tryReadChan', and
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
-- >      h <- 'tryReadNext' str
-- >      case h of
-- >        'Next' a str' -> print a >> printStream str'
-- >        -- We know that all values were already written, so a Pending tells 
-- >        -- us we can exit; in other cases we might call 'yield' and then 
-- >        -- retry that same @'tryReadNext' str@:
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
                 Just a -> return $ UT.Next a $ stream strOffset str (ix+period)

    return $ map (stream offsetInitial strInitial) $
     -- [ix0..(ix0+period-1)] -- WRONG (hint: overflow)!
        take period $ iterate (+1) ix0

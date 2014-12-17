{-# LANGUAGE BangPatterns , DeriveDataTypeable, CPP #-}
module Control.Concurrent.Chan.Unagi.NoBlocking.Unboxed.Internal
#ifdef NOT_x86
    {-# WARNING "This library is unlikely to perform well on architectures without a fetch-and-add instruction" #-}
#endif
    (sEGMENT_LENGTH
    , InChan(..), OutChan(..), ChanEnd(..), Cell, Stream(..)
    , NextSegment(..), StreamHead(..)
    , newChanStarting, writeChan, tryReadChan, readChan, Element(..)
    , dupChan
    , streamChan
    , isActive
    )
    where

-- Forked from src/Control/Concurrent/Chan/Unagi/NoBlocking/Internal.hs at
-- 9e2306330e with some code copied and modified from Unagi.Unboxed.
--
-- The main motivation for this variant is that it lets us take full advantage
-- of the atomicUnicorn trick, so in both read and write we need only use
-- sigArr when the value to be written == atomicUnicorn.
--
-- Some detailed NOTEs present in Control.Concurrent.Chan.Unagi.Unboxed have
-- been removed here although they still pertain. If you intend to work on this
-- module, please be sure you're familiar with those concerns.
--
-- TODO:
--   - Unboxed variant:
--       The "empty" cell is a magic number (somewhere far from both maxBound
--       and minBound), and need parallel array segment (actually use a cell
--       adjacent) used for disambiguating an empty cell from a written cell
--       that happens to equal magic number. Reader paths:
--          - read "non-empty" element cell value
--          - read "empty" valued cell, loadLoadBarrier (necessary?), read signal cell to ensure not magic-valued (and rarely: another barrier and re-read)
--       Writer paths:
--          - write to element cell
--          - if (hopefully) rare magic value, write "written" to signal cell

import Data.IORef
import Control.Exception
import Data.Atomics.Counter.Fat
import Data.Atomics
import qualified Data.Primitive as P
import Control.Monad
import Control.Applicative
import Data.Bits
import Data.Typeable(Typeable)
import Data.Maybe

import Control.Concurrent.Chan.Unagi.Constants

-- We can re-use much of the Unagi.Unboxed implementation here, and some of
-- Unagi.NoBlocking (at least our types, which is important):
import Control.Concurrent.Chan.Unagi.Unboxed.Internal(
          ChanEnd(..), StreamHead(..), Cell, Stream(..)
        , NextSegment(..), moveToNextCell, waitingAdvanceStream, segSource
        , cellEmpty, readElementArray, writeElementArray
        , UnagiPrim(..))
import Control.Concurrent.Chan.Unagi.NoBlocking(
          Element(..))
import qualified Control.Concurrent.Chan.Unagi.NoBlocking.Types as UT


-- | The write end of a channel created with 'newChan'.
data InChan a = InChan !(IORef Bool) -- Used for creating an OutChan in dupChan
                       !(ChanEnd a)
    deriving (Typeable)

-- | The read end of a channel created with 'newChan'.
data OutChan a = OutChan !(IORef Bool) -- Is corresponding InChan still alive?
                         !(ChanEnd a) 
    deriving (Typeable)

instance Eq (InChan a) where
    (InChan _ (ChanEnd _ headA)) == (InChan _ (ChanEnd _ headB))
        = headA == headB
instance Eq (OutChan a) where
    (OutChan _ (ChanEnd _ headA)) == (OutChan _ (ChanEnd _ headB))
        = headA == headB


newChanStarting :: (UnagiPrim a)=> Int -> IO (InChan a, OutChan a)
{-# INLINE newChanStarting #-}
newChanStarting !startingCellOffset = do
    let undefinedNewIndexedMVar = return $ -- NOTE [1]
          error "Unagi.NoBlocking.Unboxed tried to use initial fake IndexedMVar"
    stream <- uncurry Stream <$> segSource 
                             <*> undefinedNewIndexedMVar 
                             <*> newIORef NoSegment
    let end = ChanEnd
                  <$> newCounter (startingCellOffset - 1)
                  <*> newIORef (StreamHead startingCellOffset stream)
    inEnd@(ChanEnd _ inHeadRef) <- end
    finalizee <- newIORef True
    void $ mkWeakIORef inHeadRef $ do
        writeBarrier
        writeIORef finalizee False
    (,) (InChan finalizee inEnd) <$> (OutChan finalizee <$> end)
  -- [1] We reuse most of Unagi.Unboxed's internals here, but unfortunately
  -- that implementation uses a Stream type with an IndexedMVar to coordinate
  -- blocking reads. Rather than do a lot of refactoring of Unagi.Unboxed, for
  -- now we just fake it here. Unagi.Unboxed.waitingAdvanceStream will actually
  -- create new IndexedMVars for each segment, but we hope at worst that they
  -- will be GC'd immediately even when many segments-worth of elements are in
  -- the queue; the main concern is not to accumulate lots of mutable boxed
  -- objects. TODO better later, maybe.


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
    -- make sure that a peekElement that follows is not moved ahead:
    loadLoadBarrier 
    return b


-- | Duplicate a chan: the returned @OutChan@ begins empty, but data written to
-- the argument @InChan@ from then on will be available from both the original
-- @OutChan@ and the one returned here, creating a kind of broadcast channel.
--
-- See also 'streamChan' for a faster alternative that might be appropriate.
dupChan :: InChan a -> IO (OutChan a)
{-# INLINE dupChan #-}
dupChan (InChan finalizee (ChanEnd counter streamHead)) = do
    hLoc <- readIORef streamHead
    loadLoadBarrier
    wCount <- readCounter counter
    counter' <- newCounter wCount 
    streamHead' <- newIORef hLoc
    return $ OutChan finalizee $ ChanEnd counter' streamHead'


-- READING AND WRITING
--
--  We re-use the internals of Unagi.Unboxed, but use them a bit differently;
--  in particular where Unagi.Unboxed uses its SignalIntArray to indicate the
--  status of the corresponding ElementArray cell, we use it only to
--  disambiguate an unwritten cell from a written cell of a "magic" value,
--  which we'll describe below.
--  
--  When we're reading and writing values that can be written atomically (see
--  atomicUnicorn), and when that particular value is not equal to that magic
--  value we get a fast write path: simply write to the eArr. Likewise when a
--  reader reads from eArr and sees something /= atomicUnicorn, it can simply
--  return with it. In all other cases readers and writers must check in at the
--  sigArr, as in Unagi.Unboxed.

nonMagicCellWritten :: Int
nonMagicCellWritten = 1
-- and also: `cellEmpty` (imported)



--  TODO
--    - UNRELATED: check out sizeMutableArray from GHC.Prim for Bounded (rather than storing ourselves)!
--    - UNRELATED: in Counter.Fat, make constant a constant with the let/in/assert trick
--         - Double check that actually works by looking at core!


-- | Write a value to the channel.
writeChan :: UnagiPrim a=> InChan a -> a -> IO ()
{-# INLINE writeChan #-}
writeChan (InChan _ ce) = \a-> mask_ $ do 
    (segIx, (Stream sigArr eArr _ next), maybeUpdateStreamHead) <- moveToNextCell ce
    -- NOTE!: must write element both before updating stream head (see
    -- NoBlocking), and before signaling with CAS (if applicable):
    writeElementArray eArr segIx a

    let magic = atomicUnicorn
    when (isNothing magic || Just a == magic) $ do
      -- in which case a reader can't tell we've written just from a (possibly
      -- non-atomic) read from eArr:
      writeBarrier -- NOTE [1]
      P.writeByteArray sigArr segIx nonMagicCellWritten
              
    maybeUpdateStreamHead -- NOTE [2]
    -- try to pre-allocate next segment:
    when (segIx == 0) $ void $
      waitingAdvanceStream next 0
  -- [1] we need a write barrier here to make sure GHC maintains our ordering
  -- such that the element is written before we signal its availability with
  -- the write to sigArr that follows. See [2] in readChanOnExceptionUnmasked.
  --
  -- [2] Our final use of the head reference. We must make sure this IORef is
  -- not GC'd (and its finalizer run) until after our writes to the arrays
  -- above. See definition of maybeUpdateStreamHead.


-- | Read an element from the chan, returning an @'Element' a@ future which
-- returns an actual element, when available, via 'peekElement'.
--
-- /Note re. exceptions/: When an async exception is raised during a @tryReadChan@ 
-- the message that the read would have returned is likely to be lost, just as
-- it would be when raised directly after this function returns.
tryReadChan :: UnagiPrim a=> OutChan a -> IO (Element a)
{-# INLINE tryReadChan #-}
tryReadChan (OutChan _ ce) = do  -- see NoBlocking re. not masking
    (segIx, (Stream sigArr eArr _ _), maybeUpdateStreamHead) <- moveToNextCell ce
    maybeUpdateStreamHead
    return $ Element $ do
      let readElem = readElementArray eArr segIx
          slowRead = do 
             sig <- P.readByteArray sigArr segIx
             if sig == nonMagicCellWritten
               then do 
                 loadLoadBarrier -- see [1] in writeChan
                 Just <$> readElem
               else assert (sig == cellEmpty) $
                 return Nothing
      -- If we know writes of this type are atomic, we can determine if the
      -- element has been written, and possibly return it without checking
      -- sigArr.
      case atomicUnicorn of
           Just magic -> do
              el <- readElem
              if (el /= magic) 
                -- Then we know `el` was atomically written:
                then return $ Just el
                else slowRead
           Nothing -> slowRead
 

-- | @readChan io c@ returns the next element from @c@, calling 'tryReadChan'
-- and looping on the 'Element' returned, and calling @io@ at each iteration
-- when the element is not yet available. It throws 'BlockedIndefinitelyOnMVar'
-- when 'isActive' determines that a value will never be returned.
--
-- When used like @readChan 'yield'@ or @readChan ('threadDelay' 10)@ this is
-- the semantic equivalent to the blocking @readChan@ in the other
-- implementations.
readChan :: UnagiPrim a=> IO () -> OutChan a -> IO a
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
streamChan :: UnagiPrim a=> Int -> OutChan a -> IO [UT.Stream a]
{-# INLINE streamChan #-}
streamChan period (OutChan _ (ChanEnd counter streamHead)) = do
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
                go !n (Stream _ _ _ next) =
                    waitingAdvanceStream next (nEW_SEGMENT_WAIT*segIx)
                      >>= go (n-1)
            -- the stream segment holding `ix`, and its calculated offset:
            str@(Stream sigArr eArr _ _) <- go segsAway str0
            let !strOffset = offset0+(segsAway `unsafeShiftL` lOG_SEGMENT_LENGTH)  
            --                       (segsAway  *                 sEGMENT_LENGTH)
            -- Adapted from tryReadChan TODO benchmark and try to factor out:
            let readElem = readElementArray eArr segIx
                consAndStream el = return $ UT.Cons el $ stream strOffset str (ix+period)
                slowRead = do 
                   sig <- P.readByteArray sigArr segIx
                   if sig == nonMagicCellWritten
                     then do 
                       loadLoadBarrier
                       readElem >>= consAndStream
                     else assert (sig == cellEmpty) $
                       return UT.Pending
            case atomicUnicorn of
                 Just magic -> do
                    el <- readElem
                    if (el /= magic) 
                      then consAndStream el
                      else slowRead
                 Nothing -> slowRead

    return $ map (stream offsetInitial strInitial) $
     -- [ix0..(ix0+period-1)] -- WRONG (hint: overflow)!
        take period $ iterate (+1) ix0

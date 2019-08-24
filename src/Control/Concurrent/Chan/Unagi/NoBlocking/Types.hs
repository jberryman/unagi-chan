module Control.Concurrent.Chan.Unagi.NoBlocking.Types where

import Control.Applicative
import Control.Monad.Fix
import Control.Monad
import Data.Maybe

-- Mostly here to avoid unfortunate name clash with our internal Stream type


-- | An infinite stream of elements. 'tryReadNext' can be called any number of
-- times from multiple threads, and returns a value which moves monotonically
-- from 'Pending' to 'Next' if and when a head element becomes available. 
-- @isActive@ can be used to determine if the stream has expired.
newtype Stream a = Stream { tryReadNext :: IO (Next a) }

data Next a = Next a (Stream a) -- ^ The next head element along with the tail @Stream@.
            | Pending           -- ^ The next element is not yet in the queue; you can retry 'tryReadNext' until a @Next@ is returned.


-- | An @IO@ action that returns a particular enqueued element when and if it
-- becomes available. 
--
-- Each @Element@ corresponds to a particular enqueued element, i.e. a returned
-- @Element@ always offers the only means to access one particular enqueued
-- item. The value returned by @tryRead@ moves monotonically from @Nothing@
-- to @Just a@ when and if an element becomes available, and is idempotent at
-- that point.
--
-- So for instance:
--
-- @
--    (in, out) <- newChan
--    (el, _) <- tryReadChan out  -- READ FROM EMPTY CHAN
--    writeChan in "msg1"
--    writeChan in "msg2"
--    readChan out        -- RETURNS "msg2"
--    tryRead el          -- RETURNS "msg1" (which would otherwise be lost)
-- @
newtype Element a = Element { tryRead :: IO (Maybe a) }

-- Instances cribbed from MaybeT, from transformers v0.4.2.0
instance Functor Element where
    fmap f = Element . fmap (fmap f) . tryRead

instance  Applicative Element where
    pure = return
    (<*>) = ap
 
instance Alternative Element where
    empty = mzero
    (<|>) = mplus

instance Monad Element where
    fail _ = Element (return Nothing)
    return = Element . return . return
    x >>= f = Element $ do
        v <- tryRead x
        case v of
            Nothing -> return Nothing
            Just y  -> tryRead (f y)

instance MonadPlus Element where
    mzero = Element (return Nothing)
    mplus x y = Element $ do
        v <- tryRead x
        case v of
            Nothing -> tryRead y
            Just _  -> return v

instance MonadFix Element where
    mfix f = Element (mfix (tryRead . f . fromMaybe bomb))
      where bomb = error "mfix (Element): inner computation returned Nothing"

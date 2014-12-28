module Control.Concurrent.Chan.Unagi.NoBlocking.Types where

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
newtype Element a = Element { tryRead :: IO (Maybe a) }
-- TODO re-use this type when we implement tryReadChan for blocking variants
-- TODO Functor, Applicative, etc. instances

module Control.Concurrent.Chan.Unagi.NoBlocking.Types where

-- Mostly to avoid unfortunate name clash with our internal Stream type


-- | An infinite stream of elements. tryReadChan can be called any number of
-- times from multiple threads, and returns a value which moves monotonically
-- from 'Pending' to 'Cons' if a head element becomes available.
newtype Stream a = Stream { tryReadStream :: IO (Cons a) }

data Cons a = Cons a (Stream a) -- ^ The head element along with the tail @Stream@.
            | Pending           -- ^ The next element is not yet in the queue; you can retry 'tryReadStream' until a @Cons@ is returned.


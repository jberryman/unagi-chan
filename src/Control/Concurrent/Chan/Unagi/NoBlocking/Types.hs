module Control.Concurrent.Chan.Unagi.NoBlocking.Types where

-- Mostly to avoid unfortunate name clash with our internal Stream type


-- | An infinite stream of elements. 'tryNext' can be called any number of
-- times from multiple threads, and returns a value which moves monotonically
-- from 'Pending' to 'Cons' if and when a head element becomes available. 
-- @isActive@ can be used to determine if the stream has expired.
newtype Stream a = Stream { tryNext :: IO (Cons a) }

data Cons a = Cons a (Stream a) -- ^ The head element along with the tail @Stream@.
            | Pending           -- ^ The next element is not yet in the queue; you can retry 'tryNext' until a @Cons@ is returned.

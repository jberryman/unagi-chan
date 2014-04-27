module Control.Concurrent.Chan.Unagi (
    -- * Creating channels
      newChan
    , InChan(), OutChan()
    -- * Channel operations
    -- ** Reading
    , readChan
    , getChanContents
    -- ** Writing
    , writeChan
    , writeList2Chan

    -- * Async Exceptions Note
{- |
 There is a race condition in which a @throwTo known_blocked_reader_thread
e@ followed immediately by a 'writeChan' can cause the write to be lost;
i.e.  results in semantics equivalent to an asynchronous @throwTo@. 

When an async exception is raised in a thread blocked on a 'readChan' a race
condition is created in which the element of the next 'writeChan' may be lost.
Practically speaking this means that a thread @A@ raising an exception with
'throwTo' in thread @B@, where thread @B@ is in the process of a read on a
known-empty chan, may observe semantics that differ from
@Control.Concurrent.Chan@. Here is an example scenario:

>   (i,o) <- newChan
>   rid <- forkIO (readChan o)
>   throwTo rid ThreadKilled
>   writeChan i () -- this () may be lost
>   readChan o     -- ...and this may block indefinitely

In these cases 'throwTo' can be thought of as asynchronous, and the user should
implement their own exception type and handler (which might e.g. use @Mvar@ for
synchronization).
 -}
    ) where

import Control.Concurrent.Chan.Unagi.Internal
-- For 'writeList2Chan', as in vanilla Chan
import System.IO.Unsafe ( unsafeInterleaveIO ) 


newChan :: IO (InChan a, OutChan a)
newChan = newChanStarting (maxBound - 10) -- lets us test counter overflow in tests
--newChan = newChanStarting minBound

-- | Return a lazy list representing the contents of the supplied OutChan, much
-- like System.IO.hGetContents.
getChanContents :: OutChan a -> IO [a]
getChanContents ch = unsafeInterleaveIO (do
                            x  <- readChan ch
                            xs <- getChanContents ch
                            return (x:xs)
                        )

-- | Write an entire list of items to a chan type. Writes here from multiple
-- threads may be interleaved, and infinite lists are supported.
writeList2Chan :: InChan a -> [a] -> IO ()
{-# INLINABLE writeList2Chan #-}
writeList2Chan ch = sequence_ . map (writeChan ch)


-- TODO checklist
-- - implement additional tests and assertions for Unagi
-- - add a benchmark and validate Unagi approach (move from chan-benchmarks)
-- - add some more advanced behavior in notes, re. memory effects / wait-free-ness
-- - examine some behavior via eventlog; maybe create a script to do automated analysis of it.
-- - add atomic-primop tests
-- - figure out naming and module structure
--      - "fishy-chans", with code-named modules: Tako, Unagi
-- - consider travis CI
-- - either release or finish Tako variant, and set up tests for it.

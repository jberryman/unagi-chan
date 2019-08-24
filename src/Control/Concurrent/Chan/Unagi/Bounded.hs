{-# LANGUAGE CPP #-}
module Control.Concurrent.Chan.Unagi.Bounded
#ifdef NOT_x86
    {-# WARNING "This library is unlikely to perform well on architectures without a fetch-and-add instruction" #-}
#endif
#if __GLASGOW_HASKELL__ < 708
    {-# WARNING "Waking up blocked writers may be slower than desired in GHC<7.8 which makes readMVar non-blocking on full MVars. Nextidering upgrading." #-}
#endif
    (
{- | A queue with bounded size, which supports a 'writeChan' which blocks when
     the number of messages grows larger than desired. The bounds are
     maintained loosely between @n@ and @n*2@; see the caveats and descriptions
     of semantics in 'readChan' and 'writeChan' for details.
 -}
    -- * Creating channels
      newChan
    , InChan(), OutChan()
    -- * Channel operations
    -- ** Reading
    , readChan
    , readChanOnException
    , tryReadChan
    , Element(..)
    , getChanContents
    , estimatedLength
    -- ** Writing
    , writeChan
    , tryWriteChan
    , writeList2Chan
    -- ** Broadcasting
    , dupChan
    ) where

-- forked from src/Control/Concurrent/Chan/Unagi.hs 43706b2

import Control.Concurrent.Chan.Unagi.Bounded.Internal
import Control.Concurrent.Chan.Unagi.NoBlocking.Types
-- For 'writeList2Chan', as in vanilla Chan
import System.IO.Unsafe ( unsafeInterleaveIO ) 


-- | Create a new channel of the passed size, returning its write and read ends.
--
-- The passed integer bounds will be rounded up to the next highest power of
-- two, @n@. The queue may grow up to size @2*n@ (see 'writeChan' for details),
-- and the resulting chan pair requires O(n) space.
newChan :: Int -> IO (InChan a, OutChan a)
newChan size = newChanStarting (maxBound - 10) size
    -- lets us test counter overflow in tests and normal course of operation

-- | Return a lazy infinite list representing the contents of the supplied
-- OutChan, much like System.IO.hGetContents.
getChanContents :: OutChan a -> IO [a]
getChanContents ch = unsafeInterleaveIO (do
                            x  <- unsafeInterleaveIO $ readChan ch
                            xs <- getChanContents ch
                            return (x:xs)
                        )

-- | Write an entire list of items to a chan type. Writes here from multiple
-- threads may be interleaved, and infinite lists are supported.
writeList2Chan :: InChan a -> [a] -> IO ()
{-# INLINABLE writeList2Chan #-}
writeList2Chan ch = sequence_ . map (writeChan ch)

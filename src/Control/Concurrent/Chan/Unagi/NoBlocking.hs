module Control.Concurrent.Chan.Unagi (
{- | General-purpose concurrent FIFO queue. If you are trying to send messages
   of a primitive unboxed type, you may wish to use
   "Control.Concurrent.Chan.Unagi.Unboxed" which should be slightly faster and
   perform better when a queue grows very large. See also the bounded variant
   at "Control.Concurrent.Chan.Unagi.Bounded".
 -}
    -- * Creating channels
      newChan
    , InChan(), OutChan()
    -- * Channel operations
    -- ** Reading
    , readChan
    , readChanOnException
    , getChanContents
    -- ** Writing
    , writeChan
    , writeList2Chan
    -- ** Broadcasting
    , dupChan
    ) where

-- Forked from src/Control/Concurrent/Chan/Unagi.hs at 065cd68010

-- TODO additonal functions:
--   - faster write/read-many that increments counter by N

import Control.Concurrent.Chan.Unagi.Internal
-- For 'writeList2Chan', as in vanilla Chan
import System.IO.Unsafe ( unsafeInterleaveIO ) 


-- | Create a new channel, returning its write and read ends.
newChan :: IO (InChan a, OutChan a)
newChan = newChanStarting (maxBound - 10) 
    -- lets us test counter overflow in tests and normal course of operation

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

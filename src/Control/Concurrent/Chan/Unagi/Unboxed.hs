module Control.Concurrent.Chan.Unagi.Unboxed (
    -- * Creating channels
      newChan
    , InChan(), OutChan()
    , UnagiPrim(..)
    -- * Channel operations
    -- ** Reading
    , readChan
    , readChanOnException
    , tryReadChan
    , Element(..)
    , getChanContents
    -- ** Writing
    , writeChan
    , writeList2Chan
    -- ** Broadcasting
    , dupChan
    ) where

-- Forked from src/Control/Concurrent/Chan/Unagi/Internal.hs at 443465
--
-- TODO additonal functions:
--   - write functions optimized for single-writer
--   - faster write/read-many that increments counter by N
--   - this could be used (or forked) to implement an efficient MPSC concurrent
--     ByteString or Text queue (where writes could be variable-sized chunks
--     and we incrCounter accordingly) without too much trouble. Useful?
--       - likewise a SPMC concurrent bytestring consumer?
--   - ...or interop with 'vector' lib

import Control.Concurrent.Chan.Unagi.Unboxed.Internal
import Control.Concurrent.Chan.Unagi.NoBlocking.Types
-- For 'writeList2Chan', as in vanilla Chan
import System.IO.Unsafe ( unsafeInterleaveIO ) 


-- | Create a new channel, returning its write and read ends.
newChan :: UnagiPrim a=> IO (InChan a, OutChan a)
newChan = newChanStarting (maxBound - 10) 
    -- lets us test counter overflow in tests and normal course of operation

-- | Return a lazy list representing the contents of the supplied OutChan, much
-- like System.IO.hGetContents.
getChanContents :: UnagiPrim a=> OutChan a -> IO [a]
getChanContents ch = unsafeInterleaveIO (do
                            x  <- unsafeInterleaveIO $ readChan ch
                            xs <- getChanContents ch
                            return (x:xs)
                        )

-- | Write an entire list of items to a chan type. Writes here from multiple
-- threads may be interleaved, and infinite lists are supported.
writeList2Chan :: UnagiPrim a=> InChan a -> [a] -> IO ()
{-# INLINABLE writeList2Chan #-}
writeList2Chan ch = sequence_ . map (writeChan ch)

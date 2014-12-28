module Control.Concurrent.Chan.Unagi.NoBlocking.Unboxed (
{- | General-purpose concurrent FIFO queue without blocking reads, and with
   optimized variants for single-threaded producers and/or consumers.  This
   variant, and even more so the SP/SC variants, offer the lowest latency of
   all of the implementations in this library.
 -}
    -- * Creating channels
      newChan
    , InChan(), OutChan()
    , UnagiPrim(..)
    -- * Channel operations
    -- ** Reading
    , tryReadChan
    , readChan
    , Element(..)
    -- *** Utilities
    , isActive 
    -- ** Writing
    , writeChan
    , writeList2Chan
    -- ** Broadcasting
    , dupChan
    -- ** Streaming
    , Stream(..), Next(..)
    , streamChan
    ) where

-- Forked from src/Control/Concurrent/Chan/Unagi/NoBlocking.hs at 9e2306330e

-- TODO additonal functions:
--   - faster write/read-many that increments counter by N

import Control.Concurrent.Chan.Unagi.NoBlocking.Unboxed.Internal hiding (Stream)
import Control.Concurrent.Chan.Unagi.NoBlocking.Types
import Control.Concurrent.Chan.Unagi.Unboxed(UnagiPrim(..))

-- | Create a new channel, returning its write and read ends.
newChan :: UnagiPrim a=> IO (InChan a, OutChan a)
newChan = newChanStarting (maxBound - 10) 
    -- lets us test counter overflow in tests and normal course of operation


-- | Write an entire list of items to a chan type. Writes here from multiple
-- threads may be interleaved, and infinite lists are supported.
writeList2Chan :: UnagiPrim a=> InChan a -> [a] -> IO ()
{-# INLINABLE writeList2Chan #-}
writeList2Chan ch = sequence_ . map (writeChan ch)

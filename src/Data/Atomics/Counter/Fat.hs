module Data.Atomics.Counter.Fat (
      AtomicCounter()
    , newCounter
    , incrCounter
    , readCounter
    ) where

-- An atomic counter padded with 64-bytes (an x86 cache line) on either side to
-- try to avoid false sharing.

import Data.Primitive.MachDeps(sIZEOF_INT)
import Control.Monad.Primitive(RealWorld)
import Data.Primitive.ByteArray
import Data.Atomics(fetchAddIntArray)
import Control.Exception(assert)

newtype AtomicCounter = AtomicCounter (MutableByteArray RealWorld)

sIZEOF_CACHELINE :: Int
{-# INLINE sIZEOF_CACHELINE #-}
sIZEOF_CACHELINE   = 64

newCounter :: Int -> IO AtomicCounter
{-# INLINE newCounter #-}
newCounter n = do
    arr <- newAlignedPinnedByteArray 
                sIZEOF_CACHELINE
                sIZEOF_CACHELINE
    writeByteArray arr 0 n
    -- out of principle:
    assert (sIZEOF_INT < sIZEOF_CACHELINE) $
      return (AtomicCounter arr)

incrCounter :: Int -> AtomicCounter -> IO Int
{-# INLINE incrCounter #-}
incrCounter incr (AtomicCounter arr) =
    fetchAddIntArray arr 0 incr

readCounter :: AtomicCounter -> IO Int
{-# INLINE readCounter #-}
readCounter (AtomicCounter arr) = 
    readByteArray arr 0

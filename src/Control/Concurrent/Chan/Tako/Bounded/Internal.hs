{-# LANGUAGE BangPatterns  #-}
module Control.Concurrent.Chan.Tako.Bounded.Internal (
      MVarArray()
    , newMVarArray , writeMVarArray , readMVarArray
    , nextHighestPowerOfTwo
    ) where

import Control.Concurrent.MVar
import Control.Exception
import qualified Data.Primitive as P
import Control.Monad
import Control.Applicative
import Data.Bits
import Data.Word



-- We use this as part of some other implementations as well as Tako.Bounded
-- TODO more efficient unboxed implementation possible with CMM?
data MVarArray a = MVarArray !Int !(P.Array (MVar a))
--                              \ array size - 1, for bitwise mod

-- A new array of MVars of the given size, rounded up to the next power of two
-- TODO maybe a lazy IO newMVar version for large bounded channels
newMVarArray :: Int -> IO (MVarArray a)
newMVarArray !sizeDirty = do
    let !size = nextHighestPowerOfTwo sizeDirty
        !sizeMn1 = size - 1
    mArr <- P.newArray size undefined
    forM_ [0..sizeMn1] $ \i->
        newEmptyMVar >>= P.writeArray mArr i

    MVarArray sizeMn1 <$> P.unsafeFreezeArray mArr

-- TODO rename to take/put:
-- Put to the index specified, wrapping if larger than array; negative `n` OK.
writeMVarArray :: MVarArray a -> Int -> a -> IO ()
{-# INLINE writeMVarArray #-}
writeMVarArray !(MVarArray sizeMn1 arr) !n = 
      putMVar (P.indexArray arr (n .&. sizeMn1))

readMVarArray :: MVarArray a -> Int -> IO a
{-# INLINE readMVarArray #-}
readMVarArray !(MVarArray sizeMn1 arr) !n =
      takeMVar (P.indexArray arr (n .&. sizeMn1))

-- Not particularly fast; if needs moar fast see
--   http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
-- 
nextHighestPowerOfTwo :: Int -> Int
nextHighestPowerOfTwo 0 = 1
nextHighestPowerOfTwo n =  
    let !nhp2 = 2 ^ (ceiling (logBase 2 $ fromIntegral $ abs n :: Float) :: Int)
        -- ensure return value is actually a positive power of 2:
     in assert (nhp2 > 0 && popCount (fromIntegral nhp2 :: Word) == 1)
            nhp2

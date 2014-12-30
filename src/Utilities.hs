{-# LANGUAGE BangPatterns , MagicHash , UnboxedTuples #-}
module Utilities (
    -- * Utility Chans
    -- ** Indexed MVars
      IndexedMVar()
    , newIndexedMVar, putMVarIx, readMVarIx, tryReadMVarIx
    -- * Other stuff
    , nextHighestPowerOfTwo
    , touchIORef
    ) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Applicative
import Data.Bits
import Data.Word
import Data.Atomics
import Data.IORef
import GHC.Prim(touch#)
import GHC.IORef(IORef(..))
import GHC.STRef(STRef(..))
import GHC.Base(IO(..))

-- For now: a reverse-ordered assoc list; an IntMap might be better
newtype IndexedMVar a = IndexedMVar (IORef [(Int, MVar a)])

newIndexedMVar :: IO (IndexedMVar a)
newIndexedMVar = IndexedMVar <$> newIORef []



-- these really suck; sorry.

readMVarIx :: IndexedMVar a -> Int -> IO a
{-# INLINE readMVarIx #-}
readMVarIx mvIx i = do
    readMVar =<< getMVarIx mvIx i

tryReadMVarIx :: IndexedMVar a -> Int -> IO (Maybe a)
{-# INLINE tryReadMVarIx #-}
tryReadMVarIx mvIx i = do
    tryReadMVar =<< getMVarIx mvIx i

putMVarIx :: IndexedMVar a -> Int -> a -> IO ()
{-# INLINE putMVarIx #-}
putMVarIx mvIx i a = do
    flip putMVar a =<< getMVarIx mvIx i

-- NOTE: this uses atomic actions to stay async exception safe:
getMVarIx :: IndexedMVar a -> Int -> IO (MVar a)
{-# INLINE getMVarIx #-}
getMVarIx (IndexedMVar v) i = do
    -- We're right to optimistically create this for readMVarIx, but throw this
    -- away for most putMVarIx (from writers), probably.
    mv <- newEmptyMVar
    tk0 <- readForCAS v
    let go tk = do
            let !xs = peekTicket tk
            case findInsert i mv xs of
                 Left alreadyPresentMVar -> return alreadyPresentMVar
                 Right xs' -> do 
                    (success,newTk) <- casIORef v tk xs'
                    if success 
                        then return mv
                        else go newTk
    go tk0

-- Reverse-sorted:
findInsert :: Int -> mvar -> [(Int,mvar)] -> Either mvar [(Int,mvar)]
{-# INLINE findInsert #-}
findInsert i mv = ins where
    ins [] = Right [(i,mv)] 
    ins xss@((i',x):xs) = 
               case compare i i' of
                    GT -> Right $ (i,mv):xss
                    EQ -> Left x
                    LT -> fmap ((i',x):) $ ins xs


-- Not particularly fast; if needs moar fast see
--   http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
-- 
nextHighestPowerOfTwo :: Int -> Int
nextHighestPowerOfTwo 0 = 1
nextHighestPowerOfTwo n 
    | n > maxPowerOfTwo = error $ "The next power of two greater than "++(show n)++" exceeds the highest value representable by Int."
    | otherwise = 
        let !nhp2 = 2 ^ (ceiling (logBase 2 $ fromIntegral $ abs n :: Float) :: Int)
         -- ensure return value is actually a positive power of 2:
         in assert (nhp2 > 0 && popCount (fromIntegral nhp2 :: Word) == 1)
              nhp2

  where maxPowerOfTwo = (floor $ sqrt $ (fromIntegral (maxBound :: Int)::Float)) ^ (2::Int)

-- I'm not sure what happens if we try to use touch from
-- Control.Monad.Primitive on our boxed IORef (if it gets unboxed), so we do
-- this:
touchIORef :: IORef a -> IO ()
touchIORef (IORef (STRef v)) = IO $ \s -> 
    case touch# v s of 
         s' -> (# s', () #)

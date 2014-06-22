{-# LANGUAGE BangPatterns  #-}
module Utilities (
    -- * Utility Chans
    -- ** Indexed MVars
      IndexedMVar()
    , newIndexedMVar, putMVarIx, readMVarIx
    -- ** Other stuff
    , nextHighestPowerOfTwo
    ) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Applicative
import Data.Bits
import Data.Word
import Data.Atomics
import Data.IORef
import Control.Monad

-- For now: a reverse-ordered assoc list; an IntMap might be better
newtype IndexedMVar a = IndexedMVar (IORef [(Int, MVar a)])

newIndexedMVar :: IO (IndexedMVar a)
newIndexedMVar = IndexedMVar <$> newIORef []



-- these really suck;sorry.
readMVarIx :: IndexedMVar a -> Int -> IO a
{-# INLINE readMVarIx #-}
readMVarIx (IndexedMVar v) i = do
    -- we expect to have to create this, so do optimistically:
    mv <- newEmptyMVar
    tk0 <- readForCAS v
    let go tk = do
            let !xs = peekTicket tk
            case findInsert i mv xs of
                 Left alreadyPresentMVar -> readMVar alreadyPresentMVar
                 Right xs' -> do 
                    (success,newTk) <- casIORef v tk xs'
                    if success 
                        then readMVar mv
                        else go newTk
    go tk0

findInsert :: Int -> MVar a -> [(Int,MVar a)] -> Either (MVar a) [(Int,MVar a)]
{-# INLINE findInsert #-}
findInsert i mv = ins where
    ins [] = Right [a] 
    ins xss@((i',x):xs) = case compare i i' of
                    GT -> Right $ (i,mv):xss
                    EQ -> Left x
                    LT -> fmap ((i',x):) $ ins xs

find :: Int -> [(Int,MVar a)] -> Maybe (MVar a)
{-# INLINE find #-}
find k = go where
    go [] = Nothing
    go ((i',x):xs) | i == i' = Just x
                   | i < i' = Nothing
                   | otherwise = go xs

putMVarIx :: IndexedMVar a -> Int -> a -> IO ()
{-# INLINE putMVarIx #-}
putMVarIx (IndexedMVar v) i a = do
    tk0 <- readForCAS v
    case find i $ peekTicket tk0 of
         Just mv -> putMVar mv a
         Nothing -> do
            mv <- newMVar a
            let go tk = do
                    let !xs = peekTicket tk
                    (success,newTk) <- casIORef v tk ((i,mv):xs)
                    unless success $ 
                        case findInsert i mv $ peekTicket newTk of
                             Left alreadyPresentMVar -> 
                                putMVar alreadyPresentMVar a
                             Right xs' -> go newTk
            go tk0 (
    
    


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

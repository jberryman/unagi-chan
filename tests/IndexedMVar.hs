module IndexedMVar (indexedMVarMain) where

import Utilities
import Control.Monad
import Control.Concurrent
import Control.Exception(onException)
import Data.IORef

indexedMVarMain :: IO ()
indexedMVarMain = do
    putStr "Test indexedMVar... "
    replicateM_ 1000 $ 
        indexedMVarTest 128
    putStrLn "OK"

indexedMVarTest :: Int -> IO ()
indexedMVarTest n = do
    let xs = [0..n]
    mvIx <- newIndexedMVar
    start <- newIORef (0::Int)
    _ <- forkIO $ do
           writeIORef start 1
           mapM_ (\x-> putMVarIx mvIx x x `onException` (putStr "In writer: ")) xs
    
    busyWait start
    mapM_ (\x-> (readMVarIx mvIx x `onException` (putStr "In reader: ")) >>= assertEq x) xs
        where assertEq x x' = when (x /= x') $ error $ (show x)++"/="++(show x')

-- we should really abstract this out and use elsewhere
busyWait :: IORef Int -> IO ()
busyWait ref = busy where
    busy = do
         -- N.B. w/ readIORef we loop forever:
        n <- atomicModifyIORef ref (\x-> (x,x)) 
        unless (n == 1) busy

module IndexedMVar (indexedMVarMain) where

import Utilities
import Control.Monad
import Control.Concurrent
import Control.Exception(onException)
import Data.IORef

indexedMVarMain :: IO ()
indexedMVarMain = do
    putStr "Test indexedMVar... "
    replicateM_ 100 $ 
        indexedMVarTest 32
    putStrLn "OK"

-- TODO quickcheck
indexedMVarTest :: Int -> IO ()
indexedMVarTest n = do
    let xs = [0..n]
    mvIx <- newIndexedMVar
    start <- newIORef (0::Int)
    _ <- forkIO $ do
           _ <- writeIORef start 1
           mapM_ (\x-> putMVarIx mvIx x x `onException` (putStr "In writer: ")) xs
    
    busyWait start
    mapM_ (\x-> (readMVarIx mvIx x `onException` (putStr "In reader: ")) >>= assertEq x) xs
        where assertEq x x' | x == x' = return ()
                            | otherwise = error $ (show x)++"/="++(show x')


-- we should really abstract this out and use elsewhere
busyWait :: IORef Int -> IO ()
busyWait ref = busy where
    busy = do
        n <- readIORef ref
        unless (n == 1) busy

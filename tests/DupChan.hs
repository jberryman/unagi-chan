module DupChan (dupChanMain) where

-- implementation-agnostic tests of `dupChan`

import Control.Concurrent.MVar
import Control.Concurrent(forkIO,throwTo)
import Control.Exception(AsyncException(ThreadKilled))
import Control.Monad

import Implementations


dupChanMain :: IO ()
dupChanMain = do
    putStrLn "==================="
    putStrLn "Test dupChan Unagi:"
    -- ------
    putStr "    Reader/Reader... "
    replicateM_ 1000 $ dupChanTest1 unagiImpl 50000
    putStrLn "OK"
    -- ------
    putStr "    Writer/dupChan+Reader... "
    replicateM_ 1000 $ dupChanTest2 unagiImpl 10000
    putStrLn "OK"
    putStrLn "==================="
    putStrLn "Test dupChan Unagi.Unboxed:"
    -- ------
    putStr "    Reader/Reader... "
    replicateM_ 1000 $ dupChanTest1 unboxedUnagiImpl 50000
    putStrLn "OK"
    -- ------
    putStr "    Writer/dupChan+Reader... "
    replicateM_ 1000 $ dupChanTest2 unboxedUnagiImpl 10000
    putStrLn "OK"

-- Check output where dupChan at known point in input stream, with two
-- concurrent readers.
dupChanTest1 :: Implementation inc outc Int -> Int -> IO ()
dupChanTest1 (newChan,writeChan,readChan,dupChan) n = do
    let s1 = [1.. ndiv2]
        s2 = [(ndiv2+1)..n]
        ndiv2 = n `div` 2
    (i,o) <- newChan
    mapM_ (writeChan i) s1
    oDup <- dupChan i
    mapM_ (writeChan i) s2

    out <- newEmptyMVar
    outDup <- newEmptyMVar

    _ <- forkIO $ (replicateM n (readChan o) >>= putMVar out)
    _ <- forkIO $ (replicateM ndiv2 (readChan oDup) >>= putMVar outDup)

    x <- takeMVar out
    y <- takeMVar outDup

    unless (x == s1++s2) $
        error ""
    unless (y == s2) $ 
        error $ "dupChan returned unexpected results: "++(show y)

-- Check concurrent writes with dupChan + reads, check all reads are some
-- contiguous part of the input stream.
dupChanTest2 :: Implementation inc outc Int -> Int -> IO ()
dupChanTest2 (newChan,writeChan,readChan,dupChan) n = do
    (i,o) <- newChan
    out <- newEmptyMVar
    writer <- forkIO $ mapM_ (writeChan i) [(0::Int)..]
    
    s1 <- replicateM n (readChan o)
    _ <- forkIO (dupChan i >>= replicateM n . readChan >>= putMVar out)
    s2 <- replicateM n (readChan o)
    s3 <- takeMVar out

    throwTo writer ThreadKilled 

    unless (all increm [s1,s2,s3]) $ do
        print [s1,s2,s3]
        error "All read streams should be incrementing by one, without breaks"
  where increm [] = error "Fix dupChanTest2"
        increm xss@(x:_) = xss == take n [x..]

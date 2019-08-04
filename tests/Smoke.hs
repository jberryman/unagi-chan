{-# LANGUAGE BangPatterns #-}
module Smoke (smokeMain) where

import Control.Monad
import Control.Concurrent(forkIO,threadDelay,myThreadId,ThreadId)
import qualified Control.Concurrent.Chan as C
import Data.List
import Control.Exception
import qualified Control.Exception as E

import Implementations

-- TODO This is real lame, probably just use async
--      Rethrow 
forkCatching :: Bool -> String -> IO () -> IO ThreadId
forkCatching expectingBlock nm io = do
    mainTid <- myThreadId
    let lg e = do putStrLn $ "!!! EXCEPTION IN "++nm++": "++(show e) 
                  throwTo mainTid e
    forkIO $ io `E.catches` [
                E.Handler (\e -> when (not expectingBlock) $ lg (e :: BlockedIndefinitelyOnMVar))
              , E.Handler (\e -> case (e :: AsyncException) of 
                                   ThreadKilled -> return ()
                                   _ -> lg e  )
              ]

    

smokeMain :: IO ()
smokeMain = (do
    putStrLn "==================="
    putStrLn "Testing Unagi:"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unagiImpl 1000000
    putStrLn "OK"
    -- ------
    testContention unagiImpl 2 2 1000000

    putStrLn "==================="
    putStrLn "Testing Unagi (with tryReadChan):"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unagiTryReadImpl 1000000
    putStrLn "OK"
    -- ------
    testContention unagiTryReadImpl 2 2 1000000
    
    putStrLn "==================="
    putStrLn "Testing Unagi (with tryReadChan, blocking):"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unagiTryReadBlockingImpl 1000000
    putStrLn "OK"
    -- ------
    testContention unagiTryReadBlockingImpl 2 2 1000000

    putStrLn "==================="
    putStrLn "Testing Unagi.NoBlocking:"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unagiNoBlockingImpl 1000000
    putStrLn "OK"
    -- ------
    testContention unagiNoBlockingImpl 2 2 1000000


    putStrLn "==================="
    putStrLn "Testing Unagi.NoBlocking.Unboxed:"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unagiNoBlockingUnboxedImpl 1000000
    putStrLn "OK"
    -- ------
    testContention unagiNoBlockingUnboxedImpl 2 2 1000000


    putStrLn "==================="
    putStrLn "Testing Unagi.Unboxed:"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unboxedUnagiImpl 1000000
    putStrLn "OK"
    -- ------
    testContention unboxedUnagiImpl 2 2 1000000

    putStrLn "==================="
    putStrLn "Testing Unagi.Unboxed (with tryReadChan):"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unboxedUnagiTryReadImpl 1000000
    putStrLn "OK"
    -- ------
    testContention unboxedUnagiTryReadImpl 2 2 1000000


    forM_ [1, 2, 4, 1024] $ \bounds-> do
        putStrLn "==================="
        putStrLn $ "Testing Unagi.Bounded (and with tryReadChan) with bounds "++(show bounds)
        -- ------
        putStr "    FIFO smoke test... "
        fifoSmoke (unagiBoundedImpl bounds) 1000000
        -- because this is slow:
        when (bounds > 100) $ fifoSmoke (unagiBoundedTryReadImpl bounds) 1000000
        putStrLn "OK"
        -- ------
        testContention (unagiBoundedImpl bounds) 2 2 1000000
        -- because this is slow:
        when (bounds > 100) $ testContention (unagiBoundedTryReadImpl bounds) 2 2 1000000

    putStrLn "==================="
    putStrLn "Testing Unagi.Unboxed (with tryReadChan, blocking):"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unboxedUnagiTryReadBlockingImpl 1000000
    putStrLn "OK"
    -- ------
    testContention unboxedUnagiTryReadBlockingImpl 2 2 1000000


    forM_ [1, 2, 4, 1024] $ \bounds-> do
        putStrLn "==================="
        putStrLn $ "Testing Unagi.Bounded (and with tryReadChan) with bounds "++(show bounds)
        -- ------
        putStr "    FIFO smoke test... "
        fifoSmoke (unagiBoundedImpl bounds) 1000000
        -- because this is slow:
        when (bounds > 100) $ fifoSmoke (unagiBoundedTryReadBlockingImpl bounds) 1000000
        putStrLn "OK"
        -- ------
        testContention (unagiBoundedImpl bounds) 2 2 1000000
        -- because this is slow:
        when (bounds > 100) $ testContention (unagiBoundedTryReadBlockingImpl bounds) 2 2 1000000


    ) `onException` (threadDelay 1000000) -- wait for forkCatching logging

-- Run two concurrent writer threads, making sure their respective sets of
-- writes arrived in order:
fifoSmoke :: Implementation inc outc Int -> Int -> IO ()
fifoSmoke (newChan,writeChan,readChan,_) n = do
    (i,o) <- newChan
    let forkWriter p = void $ forkCatching False "fifoSmoke writeChan" $ mapM_ (writeChan i) p
    forkWriter [1..n]
    forkWriter [negate n .. -1]
    -- Give a chance for writers to work on both cores, but we need the main
    -- thread to run concurrently for bounded chans:
    threadDelay 100000

    nsOut <- replicateM (n*2) $ readChan o
    let (nsPos,nsNeg) = partition (>0) nsOut
    unless (nsPos == [1..n] && nsNeg == [negate n .. -1]) $ 
        error $ "Cough!!"++(show nsOut)

-- Break up a set of unique messages running them through multiple writers to
-- multiple readers (all concurrently), making sure they all came out the same
testContention :: Implementation inc outc Int -> Int -> Int -> Int -> IO ()
testContention (newChan,writeChan,readChan,_) writers readers n = do
  let nNice = n - rem n (lcm writers readers)
             -- e.g. [[1,2,3,4,5],[6,7,8,9,10]] for 2 2 10
      groups = map (\i-> [i.. i - 1 + nNice `quot` writers]) $ [1, (nNice `quot` writers + 1).. nNice]
                     -- force list; don't change --
  out <- C.newChan

  (i,o) <- newChan
  -- Real `readChan`s will get BlockedIndefinitelyOnMVar here, when o is dead,
  -- but we need to kill them explicitly for our *TryReadImpl:
  rIds <- replicateM readers $ forkCatching True "testContention readChan o"$ forever $
             readChan o >>= C.writeChan out
  
  putStr $ "    Sending "++(show $ length $ concat groups)++" messages, with "++(show readers)++" readers and "++(show writers)++" writers.... "
  mapM_ (forkCatching False "testContention writeChan i " . mapM_ (writeChan i)) groups

  ns <- replicateM nNice (C.readChan out)
  assertIsEmptyChan out "Expected out to be empty, first of all..."
  if sort ns == [1..nNice]
      then let d = interleaving ns
            in if d < 0.7 -- arbitrary
                 then putStrLn $ "OK, BUT WARNING: low interleaving of threads: "++(show $ d)
                 else putStrLn $ "OK" --, with interleaving pct of "++(show $ d)++" (closer to 1 means we have higher confidence in the test)."
      else error "What we put in isn't what we got out :("
  mapM_ (`throwTo` ThreadKilled) rIds


-- isEmptyChan was removed at some point, which is quite annoying:
assertIsEmptyChan :: C.Chan Int -> String -> IO ()
assertIsEmptyChan c err = do
  C.writeChan c 666666
  satan <- C.readChan c
  unless (satan == 666666) $
    error err


-- --------- Helpers:

-- approx measure of interleaving (and hence contention) in test
interleaving :: (Num a, Eq a) => [a] -> Float
interleaving [] = 0
interleaving (x:xs) =  (snd $ foldl' countNonIncr (x,0) xs) / l
  where l = fromIntegral $ length xs
        countNonIncr (x0,!cnt) x1 = (x1, if x1 == x0+1 then cnt else cnt+1)

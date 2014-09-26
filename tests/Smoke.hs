{-# LANGUAGE BangPatterns #-}
module Smoke (smokeMain) where

import Control.Monad
import Control.Concurrent(forkIO,threadDelay)
import qualified Control.Concurrent.Chan as C
import Data.List
import Control.Exception
import qualified Control.Exception as E

import Implementations

-- TODO This is real lame, probably just use async
lgErrs :: Bool -> String -> IO () -> IO ()
lgErrs expectingBlock nm = E.handle $ \e-> 
    let lg = putStrLn $ "!!! EXCEPTION IN "++nm++": "++(show e) 
    in case E.fromException e of
            Just BlockedIndefinitelyOnMVar -> when (not expectingBlock) lg
            Nothing -> lg
    

smokeMain :: IO ()
smokeMain = (do
    putStrLn "==================="
    putStrLn "Testing Unagi:"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unagiImpl 100000
    putStrLn "OK"
    -- ------
    testContention unagiImpl 2 2 1000000


    putStrLn "==================="
    putStrLn "Testing Unagi.Unboxed:"
    -- ------
    putStr "    FIFO smoke test... "
    fifoSmoke unboxedUnagiImpl 100000
    putStrLn "OK"
    -- ------
    testContention unboxedUnagiImpl 2 2 1000000


    forM_ [1, 2, 4, 1024] $ \bounds-> do
        putStrLn "==================="
        putStrLn $ "Testing Unagi.Bounded with bounds "++(show bounds)
        -- ------
        putStr "    FIFO smoke test... "
        fifoSmoke (unagiBoundedImpl bounds) 100000
        putStrLn "OK"
        -- ------
        testContention (unagiBoundedImpl bounds) 2 2 1000000

    ) `onException` (threadDelay 1000000) -- wait for lgErrs

fifoSmoke :: Implementation inc outc Int -> Int -> IO ()
fifoSmoke (newChan,writeChan,readChan,_) n = do
    (i,o) <- newChan
    -- we need to fork this for Unagi.Bounded:
    void $ forkIO $ lgErrs False "fifoSmoke writeChan " $ mapM_ (writeChan i) [1..n]
    nsOut <- replicateM n $ readChan o
    unless (nsOut == [1..n]) $
        error "Cough!"

testContention :: Implementation inc outc Int -> Int -> Int -> Int -> IO ()
testContention (newChan,writeChan,readChan,_) writers readers n = do
  let nNice = n - rem n (lcm writers readers)
             -- e.g. [[1,2,3,4,5],[6,7,8,9,10]] for 2 2 10
      groups = map (\i-> [i.. i - 1 + nNice `quot` writers]) $ [1, (nNice `quot` writers + 1).. nNice]
                     -- force list; don't change --
  out <- C.newChan

  (i,o) <- newChan
  -- some will get blocked indefinitely:
  void $ replicateM readers $ forkIO $ lgErrs True "testContention readChan o"$ forever $
      readChan o >>= C.writeChan out
  
  putStr $ "    Sending "++(show $ length $ concat groups)++" messages, with "++(show readers)++" readers and "++(show writers)++" writers.... "
  mapM_ (forkIO . lgErrs False "testContention writeChan i " . mapM_ (writeChan i)) groups

  ns <- replicateM nNice (C.readChan out)
  isEmpty <- C.isEmptyChan out
  if sort ns == [1..nNice] && isEmpty
      then let d = interleaving ns
            in if d < 0.7 -- arbitrary
                 then putStrLn $ "OK, BUT WARNING: low interleaving of threads: "++(show $ d)
                 else putStrLn $ "OK" --, with interleaving pct of "++(show $ d)++" (closer to 1 means we have higher confidence in the test)."
      else error "What we put in isn't what we got out :("

-- --------- Helpers:

-- approx measure of interleaving (and hence contention) in test
interleaving :: (Num a, Eq a) => [a] -> Float
interleaving [] = 0
interleaving (x:xs) =  (snd $ foldl' countNonIncr (x,0) xs) / l
  where l = fromIntegral $ length xs
        countNonIncr (x0,!cnt) x1 = (x1, if x1 == x0+1 then cnt else cnt+1)

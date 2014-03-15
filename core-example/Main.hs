module Main (main) where

import Control.Monad
import System.Environment
import Control.Concurrent.MVar
import Control.Concurrent
import qualified Control.Concurrent.Chan.Split as F

import qualified Control.Concurrent.Chan as C
import qualified Control.Concurrent.STM.TQueue as S
import Control.Concurrent.STM

import Debug.Trace

-- This is a copy of the "async 100 writers 100 readers" from chan-benchmarks
main = do 
    (nm:other) <- getArgs
    let n = 1000000
        (r,w) = case other of
                     [rS,wS] -> (read rS, read wS)
                     _       -> (100,100)
    putStrLn $ "Running with "++show r++" readers, and "++ show w++" writers."
    case nm of
         "fast" -> runF w r n
         "chan" -> runC w r n
         "stm"  -> runS w r n


{-
Notes:
    stm:
        throws OOM on 100x100
    fast: 
        memory profiles are all over the map between runs
        best profile was when with Running with 1 readers, and 100 writers  !!
-}

-- TODO fix this up with CPP for cleaner core
runF :: Int -> Int -> Int -> IO ()
runF writers readers n = do
  let nNice = n - rem n (lcm writers readers)
      perReader = nNice `quot` readers
      perWriter = (nNice `quot` writers)
  vs <- replicateM readers newEmptyMVar
  (i,o) <- F.newSplitChan
  let doRead = replicateM_ perReader $ theRead
      theRead = F.readChan o
      doWrite = replicateM_ perWriter $ theWrite
      theWrite = F.writeChan i (1 :: Int)
  mapM_ (\v-> forkIO (traceEventIO "READER START" >> doRead >> putMVar v ())) vs

  wWaits <- replicateM writers newEmptyMVar
  mapM_ (\v-> forkIO $ (takeMVar v >> traceEventIO "WRITER START" >> doWrite)) wWaits
  mapM_ (\v-> putMVar v ()) wWaits

  mapM_ takeMVar vs -- await readers

-- ------------------------------------------------
-- FOR COMPARISON:

runS :: Int -> Int -> Int -> IO ()
runS writers readers n = do
  let nNice = n - rem n (lcm writers readers)
      perReader = nNice `quot` readers
      perWriter = (nNice `quot` writers)
  vs <- replicateM readers newEmptyMVar
  tq <- S.newTQueueIO
  let doRead = replicateM_ perReader $ theRead
      theRead = (atomically . S.readTQueue) tq
      doWrite = replicateM_ perWriter $ theWrite
      theWrite = atomically $ S.writeTQueue tq (1 :: Int)
  mapM_ (\v-> forkIO (traceEventIO "READER START" >> doRead >> putMVar v ())) vs

  wWaits <- replicateM writers newEmptyMVar
  mapM_ (\v-> forkIO $ (takeMVar v >> traceEventIO "WRITER START" >> doWrite)) wWaits
  mapM_ (\v-> putMVar v ()) wWaits

  mapM_ takeMVar vs -- await readers

runC :: Int -> Int -> Int -> IO ()
runC writers readers n = do
  let nNice = n - rem n (lcm writers readers)
      perReader = nNice `quot` readers
      perWriter = (nNice `quot` writers)
  vs <- replicateM readers newEmptyMVar
  c <- C.newChan
  let doRead = replicateM_ perReader $ theRead
      theRead = C.readChan c
      doWrite = replicateM_ perWriter $ theWrite
      theWrite = C.writeChan c (1 :: Int)
  mapM_ (\v-> forkIO (traceEventIO "READER START" >> doRead >> putMVar v ())) vs

  wWaits <- replicateM writers newEmptyMVar
  mapM_ (\v-> forkIO $ (takeMVar v >> traceEventIO "WRITER START" >> doWrite)) wWaits
  mapM_ (\v-> putMVar v ()) wWaits

  mapM_ takeMVar vs -- await readers


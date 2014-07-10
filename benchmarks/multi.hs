{-# LANGUAGE BangPatterns #-}
module Main where

import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Unboxed as UU
#ifdef COMPARE_BENCHMARKS
import Control.Concurrent.Chan
import Control.Concurrent.STM
--import qualified Data.Concurrent.Queue.MichaelScott as MS
#endif

import Control.Concurrent.MVar
import Control.Concurrent.Async
import Control.Monad
import Criterion.Main
import GHC.Conc


main :: IO ()
main = do 
-- save some time and don't let other chans choke:
#ifdef COMPARE_BENCHMARKS
  let n = 100000
#else
  let n = 100000 -- TODO 1mil ?
#endif

  procs <- getNumCapabilities
  unless (even procs) $
    error "Please run with +RTS -NX, where X is an even number"
  let procs_div2 = procs `div` 2
  if procs_div2 >= 0 then return ()
                     else error "Run with RTS +N2 or more"

  putStrLn $ "Running with capabilities: "++(show procs)

  (fill_empty_fastUI, fill_empty_fastUO) <- U.newChan
  (fill_empty_fastUUI, fill_empty_fastUUO) <- UU.newChan -- TODO WHY IS THIS COMPILING BELOW???
#ifdef COMPARE_BENCHMARKS
  fill_empty_chan <- newChan
  fill_empty_tqueue <- newTQueueIO
  --fill_empty_tbqueue <- newTBQueueIO maxBound
  --fill_empty_lockfree <- MS.newQ
#endif

  defaultMain $
    [ bgroup ("Operations on "++(show n)++" messages") $
        [ bgroup "unagi-chan Unagi" $
              -- this gives us a measure of effects of contention between
              -- readers and writers when compared with single-threaded
              -- version:
              [ bench "async 1 writers 1 readers" $ asyncReadsWritesUnagi 1 1 n
              -- This is measuring the effects of bottlenecks caused by
              -- descheduling, context-switching overhead (forced by
              -- fairness properties in the case of MVar), as well as
              -- all of the above; this is probably less than
              -- informative. Try threadscope on a standalone test:
              , bench "oversubscribing: async 100 writers 100 readers" $ asyncReadsWritesUnagi 100 100 n
              -- NOTE: this is a bit hackish, filling in one test and
              -- reading in the other; make sure memory usage isn't
              -- influencing mean:
              -- This measures writer/writer contention:
              , bench ("async "++(show procs)++" writers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (U.writeChan fill_empty_fastUI ()) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              -- This measures reader/reader contention:
              , bench ("async "++(show procs)++" readers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (U.readChan fill_empty_fastUO) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones

              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagi n
              ]
        , bgroup "unagi-chan Unagi.Unboxed" $
              [ bench "async 1 writers 1 readers" $ asyncReadsWritesUnagiUnboxed 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ asyncReadsWritesUnagiUnboxed 100 100 n
              -- TODO using Ints here instead of (); change others so we can properly compare?
              , bench ("async "++(show procs)++" writers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (UU.writeChan fill_empty_fastUUI (0::Int)) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              , bench ("async "++(show procs)++" readers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (UU.readChan fill_empty_fastUUO) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones

              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagiUnboxed n
              ]
#ifdef COMPARE_BENCHMARKS
        , bgroup "Chan" $
              [ bench "async 1 writer 1 readers" $ asyncReadsWritesChan 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ asyncReadsWritesChan 100 100 n
              , bench ("async "++(show procs)++" writers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (writeChan fill_empty_chan ()) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              -- This measures reader/reader contention:
              , bench ("async "++(show procs)++" readers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (readChan fill_empty_chan) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              ]
        , bgroup "TQueue" $
              [ bench "async 1 writers 1 readers" $ asyncReadsWritesTQueue 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ asyncReadsWritesTQueue 100 100 n
              -- This measures writer/writer contention:
              , bench ("async "++(show procs)++" writers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (atomically $ writeTQueue fill_empty_tqueue ()) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              -- This measures reader/reader contention:
              , bench ("async "++(show procs)++" readers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (atomically $ readTQueue fill_empty_tqueue) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              ]
        {-
        , bgroup "TBQueue" $
              [ bench "async 1 writers 1 readers" $ asyncReadsWritesTBQueue 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ asyncReadsWritesTBQueue 100 100 n
              -- This measures writer/writer contention:
              , bench ("async "++(show procs)++" writers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (atomically $ writeTBQueue fill_empty_tbqueue ()) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              -- This measures reader/reader contention:
              , bench ("async "++(show procs)++" readers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (atomically $ readTBQueue fill_empty_tbqueue) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              ]
        -- michael-scott queue implementation, using atomic-primops
        , bgroup "lockfree-queue" $
              [ bench "async 1 writer 1 readers" $ asyncReadsWritesLockfree 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ asyncReadsWritesLockfree 100 100 n
              , bench ("async "++(show procs)++" writers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (MS.pushL fill_empty_lockfree ()) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              , bench ("async "++(show procs)++" readers") $ do
                  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
                  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (msreadR fill_empty_lockfree) >> putMVar done1 ()) $ zip starts dones
                  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
              ]
         -}
#endif
         ]
#ifdef COMPARE_BENCHMARKS
    -- This is our set of benchmarks we use to create the graph we'll use in
    -- the haddocks to demo performance
    , bgroup ("Demo with messages x"++show n) $
        let runs = [1..procs_div2]
            benchRun c = bench ("with "++(show c)++ "readers and "++(show c)++" writers")
         in [ bgroup "Unagi        " $
                map (\c-> benchRun c $ asyncReadsWritesUnagi c c n) runs
            , bgroup "Unagi.Unboxed" $
                map (\c-> benchRun c $ asyncReadsWritesUnagiUnboxed c c n) runs
            , bgroup "TQueue       " $
                map (\c-> benchRun c $ asyncReadsWritesTQueue c c n) runs
            , bgroup "Chan         " $
                map (\c-> benchRun c $ asyncReadsWritesChan c c n) runs
            ]
#endif
    ]


-- TODO maybe factor out reads/writes/news, and hope they get inlined

asyncReadsWritesUnagi :: Int -> Int -> Int -> IO ()
asyncReadsWritesUnagi writers readers n = do
  let nNice = n - rem n (lcm writers readers)
  (i,o) <- U.newChan
  rcvrs <- replicateM readers $ async $ replicateM_ (nNice `quot` readers) $ U.readChan o
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ U.writeChan i ()
  mapM_ wait rcvrs

-- A slightly more realistic benchmark, lets us see effects of unboxed strict
-- in value, and inlining effects w/ partially applied writeChan
asyncSumIntUnagi :: Int -> IO Int
asyncSumIntUnagi n = do
   (i,o) <- U.newChan
   let readerSum  0  !tot = return tot
       readerSum !n' !tot = U.readChan o >>= readerSum (n'-1) . (tot+)
   _ <- async $ mapM_ (U.writeChan i) [1..n] -- NOTE: partially-applied writeChan
   readerSum n 0



-- Unboxed Unagi:
-- NOTE: using Int here instead of (). TODO change others so we can properly compare?
asyncReadsWritesUnagiUnboxed :: Int -> Int -> Int -> IO ()
asyncReadsWritesUnagiUnboxed writers readers n = do
  let nNice = n - rem n (lcm writers readers)
  (i,o) <- UU.newChan
  rcvrs <- replicateM readers $ async $ replicateM_ (nNice `quot` readers) $ UU.readChan o
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ UU.writeChan i (0::Int)
  mapM_ wait rcvrs

asyncSumIntUnagiUnboxed :: Int -> IO Int
asyncSumIntUnagiUnboxed n = do
   (i,o) <- UU.newChan
   let readerSum  0  !tot = return tot
       readerSum !n' !tot = UU.readChan o >>= readerSum (n'-1) . (tot+)
   _ <- async $ mapM_ (UU.writeChan i) [1..n] -- NOTE: partially-applied writeChan
   readerSum n 0



#ifdef COMPARE_BENCHMARKS

asyncReadsWritesChan :: Int -> Int -> Int -> IO ()
asyncReadsWritesChan writers readers n = do
  let nNice = n - rem n (lcm writers readers)
  c <- newChan
  rcvrs <- replicateM readers $ async $ replicateM_ (nNice `quot` readers) $ readChan c
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ writeChan c ()
  mapM_ wait rcvrs


asyncReadsWritesTQueue :: Int -> Int -> Int -> IO ()
asyncReadsWritesTQueue writers readers n = do
  let nNice = n - rem n (lcm writers readers)
  c <- newTQueueIO
  rcvrs <- replicateM readers $ async $ replicateM_ (nNice `quot` readers) $ atomically $ readTQueue c
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ atomically $ writeTQueue c ()
  mapM_ wait rcvrs


{-
-- lockfree-queue
asyncReadsWritesLockfree :: Int -> Int -> Int -> IO ()
asyncReadsWritesLockfree writers readers n = do
  let nNice = n - rem n (lcm writers readers)
  c <- MS.newQ
  rcvrs <- replicateM readers $ async $ replicateM_ (nNice `quot` readers) $ msreadR c
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ MS.pushL c ()
  mapM_ wait rcvrs

-- a busy-blocking read:
msreadR :: MS.LinkedQueue a -> IO a
msreadR q = MS.tryPopR q >>= maybe (msreadR q) return

-- TBQueue
asyncReadsWritesTBQueue :: Int -> Int -> Int -> IO ()
asyncReadsWritesTBQueue writers readers n = do
  let nNice = n - rem n (lcm writers readers)
  c <- newTBQueueIO 4096
  rcvrs <- replicateM readers $ async $ replicateM_ (nNice `quot` readers) $ atomically $ readTBQueue c
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ atomically $ writeTBQueue c ()
  mapM_ wait rcvrs
-}

#endif

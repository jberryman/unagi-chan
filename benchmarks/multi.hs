{-# LANGUAGE BangPatterns #-}
module Main where

import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Unboxed as UU
import qualified Control.Concurrent.Chan.Unagi.Bounded as UB
import qualified Control.Concurrent.Chan.Unagi.NoBlocking as UN
import qualified Control.Concurrent.Chan.Unagi.NoBlocking as UNU
#ifdef COMPARE_BENCHMARKS
import Control.Concurrent.Chan
import Control.Concurrent.STM
--import qualified Data.Concurrent.Queue.MichaelScott as MS
#endif

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

  defaultMain $
    [ bgroup ("Operations on "++(show n)++" messages") $
        [ bgroup "unagi-chan Unagi" $
              -- this gives us a measure of effects of contention between
              -- readers and writers when compared with single-threaded
              -- version:
              [ bench "async 1 writers 1 readers" $ nfIO $ asyncReadsWritesUnagi 1 1 n
              -- This is measuring the effects of bottlenecks caused by
              -- descheduling, context-switching overhead (forced by
              -- fairness properties in the case of MVar), as well as
              -- all of the above; this is probably less than
              -- informative. Try threadscope on a standalone test:
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesUnagi 100 100 n
              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagi n
              ]
        , bgroup "unagi-chan Unagi.Unboxed" $
              [ bench "async 1 writers 1 readers" $ nfIO $ asyncReadsWritesUnagiUnboxed 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesUnagiUnboxed 100 100 n
              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagiUnboxed n
              ]
        , bgroup "unagi-chan Unagi.Bounded" $
              [ bench "async 1 writers 1 readers" $ nfIO $ asyncReadsWritesUnagiBounded 4096 1 1 n   -- TODO with different bounds, here and below
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesUnagiBounded 4096 100 100 n
              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagiBounded 4096 n -- TODO with different bounds
              ]
        , bgroup "unagi-chan Unagi.NoBlocking" $
              [ bench "async 1 writers 1 readers" $ nfIO $ asyncReadsWritesUnagiNoBlocking 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesUnagiNoBlocking 100 100 n
              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagiNoBlocking n
              ]
        , bgroup "unagi-chan Unagi.NoBlocking Stream" $
              [ bench "async 1 writers 1 readers" $ nfIO $ asyncReadsWritesUnagiNoBlockingStream 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesUnagiNoBlockingStream 100 100 n
              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagiNoBlockingStream n
              ]
        , bgroup "unagi-chan Unagi.NoBlocking.Unboxed" $
              [ bench "async 1 writers 1 readers" $ nfIO $ asyncReadsWritesUnagiNoBlockingUnboxed 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesUnagiNoBlockingUnboxed 100 100 n
              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagiNoBlockingUnboxed n
              ]
        , bgroup "unagi-chan Unagi.NoBlocking.Unboxed Stream" $
              [ bench "async 1 writers 1 readers" $ nfIO $ asyncReadsWritesUnagiNoBlockingUnboxedStream 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesUnagiNoBlockingUnboxedStream 100 100 n
              , bench "async Int writer, main thread read and sum" $ nfIO $ asyncSumIntUnagiNoBlockingUnboxedStream n
              ]
#ifdef COMPARE_BENCHMARKS
        , bgroup "Chan" $
              [ bench "async 1 writer 1 readers" $ nfIO $ asyncReadsWritesChan 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesChan 100 100 n
              ]
        , bgroup "TQueue" $
              [ bench "async 1 writers 1 readers" $ nfIO $ asyncReadsWritesTQueue 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ nfIO $ asyncReadsWritesTQueue 100 100 n
              ]
        {-
        , bgroup "TBQueue" $
              [ bench "async 1 writers 1 readers" $ asyncReadsWritesTBQueue 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ asyncReadsWritesTBQueue 100 100 n
              -- This measures writer/writer contention:
              ]
        -- michael-scott queue implementation, using atomic-primops
        , bgroup "lockfree-queue" $
              [ bench "async 1 writer 1 readers" $ asyncReadsWritesLockfree 1 1 n
              , bench "oversubscribing: async 100 writers 100 readers" $ asyncReadsWritesLockfree 100 100 n
              ]
         -}
#endif
         ]
#ifdef COMPARE_BENCHMARKS
    -- This is our set of benchmarks we use to create the graph we'll use in
    -- the haddocks to demo performance
    , bgroup ("Demo with messages x"++show n) $
        let runs = [1..procs_div2]
            benchRun c = bench ("with "++(show c)++ " readers and "++(show c)++" writers") . nfIO
         in [ bgroup "Unagi        " $
                map (\c-> benchRun c $ asyncReadsWritesUnagi c c n) runs
            , bgroup "Unagi.Unboxed" $
                map (\c-> benchRun c $ asyncReadsWritesUnagiUnboxed c c n) runs
            , bgroup "Unagi.Bounded (4096)" $
                map (\c-> benchRun c $ asyncReadsWritesUnagiBounded 4096 c c n) runs -- TODO with different bounds.
            , bgroup "Unagi.NoBlocking" $
                map (\c-> benchRun c $ asyncReadsWritesUnagiNoBlocking c c n) runs
            , bgroup "Unagi.NoBlocking Stream" $
                map (\c-> benchRun c $ asyncReadsWritesUnagiNoBlockingStream c c n) runs
            , bgroup "Unagi.NoBlocking.Unboxed" $
                map (\c-> benchRun c $ asyncReadsWritesUnagiNoBlockingUnboxed c c n) runs
            , bgroup "Unagi.NoBlocking.Unboxed Stream" $
                map (\c-> benchRun c $ asyncReadsWritesUnagiNoBlockingUnboxedStream c c n) runs
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

-- -------------------------
-- NoBlocking variant:
asyncReadsWritesUnagiNoBlocking :: Int -> Int -> Int -> IO ()
asyncReadsWritesUnagiNoBlocking writers readers n = do
  -- A fairly reasonable heuristic: yield if we're oversubscribed, else do threadDelay:
  procs <- getNumCapabilities
  let pause = if (readers+writers) > procs then yield else threadDelay 1

  let nNice = n - rem n (lcm writers readers)
  (i,o) <- UN.newChan
  rcvrs <- replicateM readers $ async $ 
             replicateM_ (nNice `quot` readers) $ 
               UN.readChan pause o
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ UN.writeChan i ()
  mapM_ wait rcvrs

-- A slightly more realistic benchmark, lets us see effects of unboxed strict
-- in value, and inlining effects w/ partially applied writeChan
asyncSumIntUnagiNoBlocking :: Int -> IO Int
asyncSumIntUnagiNoBlocking n = do
   (i,o) <- UN.newChan
   let readerSum  0  !tot = return tot
       readerSum !n' !tot = UN.readChan (threadDelay 1) o 
                              >>= readerSum (n'-1) . (tot+)
   _ <- async $ mapM_ (UN.writeChan i) [1..n] -- NOTE: partially-applied writeChan
   readerSum n 0

-- Unagi.NoBlocking Stream interface:
asyncReadsWritesUnagiNoBlockingStream :: Int -> Int -> Int -> IO ()
asyncReadsWritesUnagiNoBlockingStream writers readers n = do
  -- A fairly reasonable heuristic: yield if we're oversubscribed, else do threadDelay:
  procs <- getNumCapabilities
  let pause = if (readers+writers) > procs then yield else threadDelay 1

  let nNice = n - rem n (lcm writers readers)
  (i,o) <- UN.newChan
  strms <- UN.streamChan readers o
  let doReads x str = when (x > 0) $ do
        cns <- UN.tryReadNext str
        case cns of
             UN.Pending -> pause >> doReads x str
             UN.Next _ str' -> doReads (x-1) str'
  rcvrs <- mapM (async . doReads (nNice `quot` readers)) strms
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ UN.writeChan i ()
  mapM_ wait rcvrs

-- A slightly more realistic benchmark, lets us see effects of unboxed strict
-- in value, and inlining effects w/ partially applied writeChan
asyncSumIntUnagiNoBlockingStream :: Int -> IO Int
asyncSumIntUnagiNoBlockingStream n = do
   (i,o) <- UN.newChan
   [ str0 ] <- UN.streamChan 1 o
   let readerSum  0  !tot _   = return tot
       readerSum !n' !tot str = do 
         cns <- UN.tryReadNext str
         case cns of
              UN.Pending -> threadDelay 1 >> readerSum n' tot str
              UN.Next val str' -> readerSum (n'-1) (tot+val) str'
   _ <- async $ mapM_ (UN.writeChan i) [1..n] -- NOTE: partially-applied writeChan
   readerSum n 0 str0




-- -------------------------
-- NoBlocking.Unboxed variant:
asyncReadsWritesUnagiNoBlockingUnboxed :: Int -> Int -> Int -> IO ()
asyncReadsWritesUnagiNoBlockingUnboxed writers readers n = do
  -- A fairly reasonable heuristic: yield if we're oversubscribed, else do threadDelay:
  procs <- getNumCapabilities
  let pause = if (readers+writers) > procs then yield else threadDelay 1

  let nNice = n - rem n (lcm writers readers)
  (i,o) <- UNU.newChan
  rcvrs <- replicateM readers $ async $ 
             replicateM_ (nNice `quot` readers) $ 
               UNU.readChan pause o
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ UNU.writeChan i ()
  mapM_ wait rcvrs

-- A slightly more realistic benchmark, lets us see effects of unboxed strict
-- in value, and inlining effects w/ partially applied writeChan
asyncSumIntUnagiNoBlockingUnboxed :: Int -> IO Int
asyncSumIntUnagiNoBlockingUnboxed n = do
   (i,o) <- UNU.newChan
   let readerSum  0  !tot = return tot
       readerSum !n' !tot = UNU.readChan (threadDelay 1) o 
                              >>= readerSum (n'-1) . (tot+)
   _ <- async $ mapM_ (UNU.writeChan i) [1..n] -- NOTE: partially-applied writeChan
   readerSum n 0

-- Unagi.NoBlocking.Unboxed Stream interface:
asyncReadsWritesUnagiNoBlockingUnboxedStream :: Int -> Int -> Int -> IO ()
asyncReadsWritesUnagiNoBlockingUnboxedStream writers readers n = do
  -- A fairly reasonable heuristic: yield if we're oversubscribed, else do threadDelay:
  procs <- getNumCapabilities
  let pause = if (readers+writers) > procs then yield else threadDelay 1

  let nNice = n - rem n (lcm writers readers)
  (i,o) <- UNU.newChan
  strms <- UNU.streamChan readers o
  let doReads x str = when (x > 0) $ do
        cns <- UNU.tryReadNext str
        case cns of
             UNU.Pending -> pause >> doReads x str
             UNU.Next _ str' -> doReads (x-1) str'
  rcvrs <- mapM (async . doReads (nNice `quot` readers)) strms
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ UNU.writeChan i ()
  mapM_ wait rcvrs

-- A slightly more realistic benchmark, lets us see effects of unboxed strict
-- in value, and inlining effects w/ partially applied writeChan
asyncSumIntUnagiNoBlockingUnboxedStream :: Int -> IO Int
asyncSumIntUnagiNoBlockingUnboxedStream n = do
   (i,o) <- UNU.newChan
   [ str0 ] <- UNU.streamChan 1 o
   let readerSum  0  !tot _   = return tot
       readerSum !n' !tot str = do 
         cns <- UNU.tryReadNext str
         case cns of
              UNU.Pending -> threadDelay 1 >> readerSum n' tot str
              UNU.Next val str' -> readerSum (n'-1) (tot+val) str'
   _ <- async $ mapM_ (UNU.writeChan i) [1..n] -- NOTE: partially-applied writeChan
   readerSum n 0 str0



-- -------------------------
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


-- -------------------------
-- Bounded Unagi:
-- NOTE: using Int here instead of (). TODO change others so we can properly compare?
asyncReadsWritesUnagiBounded :: Int -> Int -> Int -> Int -> IO ()
asyncReadsWritesUnagiBounded bnds writers readers n = do
  let nNice = n - rem n (lcm writers readers)
  (i,o) <- UB.newChan bnds
  rcvrs <- replicateM readers $ async $ replicateM_ (nNice `quot` readers) $ UB.readChan o
  _ <- replicateM writers $ async $ replicateM_ (nNice `quot` writers) $ UB.writeChan i (0::Int)
  mapM_ wait rcvrs

asyncSumIntUnagiBounded :: Int -> Int -> IO Int
asyncSumIntUnagiBounded bnds n = do
   (i,o) <- UB.newChan bnds
   let readerSum  0  !tot = return tot
       readerSum !n' !tot = UB.readChan o >>= readerSum (n'-1) . (tot+)
   _ <- async $ mapM_ (UB.writeChan i) [1..n] -- NOTE: partially-applied writeChan
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

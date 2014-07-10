module Main
    where 


import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Unboxed as UU
#ifdef COMPARE_BENCHMARKS
import Control.Concurrent.Chan
import Control.Concurrent.STM
--import qualified Data.Concurrent.Queue.MichaelScott as MS
#endif

import Control.Monad
import Criterion.Main


main :: IO ()
main = do 
-- save some time and don't let other chans choke:
#ifdef COMPARE_BENCHMARKS
  let n = 100000
#else
  let n = 1000000
#endif

  (fastEmptyUI,fastEmptyUO) <- U.newChan
  (fastEmptyUUI,fastEmptyUUO) <- UU.newChan
#ifdef COMPARE_BENCHMARKS
  chanEmpty <- newChan
  tqueueEmpty <- newTQueueIO
  --tbqueueEmpty <- newTBQueueIO 2
  --lockfreeQEmpty <- MS.newQ
#endif

  defaultMain $
    -- Very artificial; just adding up the costs of the takes/puts/reads
    -- involved in getting a single message in and out
    [ bgroup "Latency micro-benchmark" $
        [ bench "unagi-chan Unagi" (U.writeChan fastEmptyUI () >> U.readChan fastEmptyUO)
        , bench "unagi-chan Unagi.Unboxed" (UU.writeChan fastEmptyUUI (0::Int) >> UU.readChan fastEmptyUUO) -- TODO comparing Int writing to (). Change?
#ifdef COMPARE_BENCHMARKS
        , bench "Chan" (writeChan chanEmpty () >> readChan chanEmpty)
        , bench "TQueue" (atomically (writeTQueue tqueueEmpty () >>  readTQueue tqueueEmpty))
        {-
        -- TODO when comparing our bounded queues:
        , bench "TBQueue" (atomically (writeTBQueue tbqueueEmpty () >>  readTBQueue tbqueueEmpty))
        -- TODO when works with 7.8
        , bench "lockfree-queue" (MS.pushL lockfreeQEmpty () >> msreadR lockfreeQEmpty)
        -}
#endif
        ]
    , bgroup ("Throughput with "++show n++" messages") $
        [ bgroup "sequential write all then read all" $
              [ bench "unagi-chan Unagi" $ runtestSplitChanU1 n
              , bench "unagi-chan Unagi.Unboxed" $ runtestSplitChanUU1 n
#ifdef COMPARE_BENCHMARKS
              , bench "Chan" $ runtestChan1 n
              , bench "TQueue" $ runtestTQueue1 n
           -- , bench "TBQueue" $ runtestTBQueue1 n
           -- , bench "lockfree-queue" $ runtestLockfreeQueue1 n
#endif
              ]
        , bgroup "repeated write some, read some" $ 
              [ bench "unagi-chan Unagi" $ runtestSplitChanU2 n
              , bench "unagi-chan Unagi.Unboxed" $ runtestSplitChanUU2 n
#ifdef COMPARE_BENCHMARKS
              , bench "Chan" $ runtestChan2 n
              , bench "TQueue" $ runtestTQueue2 n
           -- , bench "TBQueue" $ runtestTBQueue2 n
           -- , bench "lockfree-queue" $ runtestLockfreeQueue2 n
#endif
              ]
        ]
    ]

-- unagi-chan Unagi --
runtestSplitChanU1, runtestSplitChanU2 :: Int -> IO ()
runtestSplitChanU1 n = do
  (i,o) <- U.newChan
  replicateM_ n $ U.writeChan i ()
  replicateM_ n $ U.readChan o

runtestSplitChanU2 n = do
  (i,o) <- U.newChan
  let n1000 = n `quot` 1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ U.writeChan i ()
    replicateM_ n1000 $ U.readChan o


-- unagi-chan Unagi Unboxed --
-- TODO comparing () to Int. Change everywhere?
runtestSplitChanUU1, runtestSplitChanUU2 :: Int -> IO ()
runtestSplitChanUU1 n = do
  (i,o) <- UU.newChan
  replicateM_ n $ UU.writeChan i (0::Int)
  replicateM_ n $ UU.readChan o

runtestSplitChanUU2 n = do
  (i,o) <- UU.newChan
  let n1000 = n `quot` 1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ UU.writeChan i (0::Int)
    replicateM_ n1000 $ UU.readChan o




#ifdef COMPARE_BENCHMARKS
-- ----------
-- Chan
runtestChan1, runtestChan2 :: Int -> IO ()
runtestChan1 n = do
  c <- newChan
  replicateM_ n $ writeChan c ()
  replicateM_ n $ readChan c

runtestChan2 n = do
  c <- newChan
  let n1000 = n `quot` 1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ writeChan c ()
    replicateM_ n1000 $ readChan c


-- ----------
-- TQueue

runtestTQueue1, runtestTQueue2 :: Int -> IO ()
runtestTQueue1 n = do
  c <- newTQueueIO
  replicateM_ n $ atomically $ writeTQueue c ()
  replicateM_ n $ atomically $ readTQueue c

runtestTQueue2 n = do
  c <- newTQueueIO
  let n1000 = n `quot` 1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ atomically $ writeTQueue c ()
    replicateM_ n1000 $ atomically $ readTQueue c

{-
-- ----------
-- TBQueue
runtestTBQueue1, runtestTBQueue2 :: Int -> IO ()
runtestTBQueue1 n = do
  c <- newTBQueueIO n -- The original benchmark must have blocked indefinitely here, no?
  replicateM_ n $ atomically $ writeTBQueue c ()
  replicateM_ n $ atomically $ readTBQueue c

runtestTBQueue2 n = do
  c <- newTBQueueIO 4096
  let n1000 = n `quot` 1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ atomically $ writeTBQueue c ()
    replicateM_ n1000 $ atomically $ readTBQueue c

-- ----------
-- from "lockfree-queue"
runtestLockfreeQueue1, runtestLockfreeQueue2 :: Int -> IO ()
runtestLockfreeQueue1 n = do
  c <- MS.newQ
  replicateM_ n $ MS.pushL c ()
  replicateM_ n $ msreadR c

runtestLockfreeQueue2 n = do
  c <- MS.newQ
  let n1000 = n `quot` 1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ MS.pushL c ()
    replicateM_ n1000 $ msreadR c

-- a busy-blocking read:
msreadR :: MS.LinkedQueue a -> IO a
msreadR q = MS.tryPopR q >>= maybe (msreadR q) return
-}
#endif

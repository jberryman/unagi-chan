module Main
    where 


import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Unboxed as UU
import qualified Control.Concurrent.Chan.Unagi.Bounded as UB
import qualified Control.Concurrent.Chan.Unagi.NoBlocking as UN
import qualified Control.Concurrent.Chan.Unagi.NoBlocking.Unboxed as UNU
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
  (fastEmptyUBI,fastEmptyUBO) <- UB.newChan 1024 -- only needs to be 1, but do apples-to-apples by matching sEGMENT_SIZE of other implementations
  (fastEmptyUNI,fastEmptyUNO) <- UN.newChan
  (fastEmptyUNUI,fastEmptyUNUO) <- UNU.newChan
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
        [ bench "unagi-chan Unagi" $ nfIO (U.writeChan fastEmptyUI () >> U.readChan fastEmptyUO)
        , bench "unagi-chan Unagi.Unboxed" $ nfIO (UU.writeChan fastEmptyUUI (0::Int) >> UU.readChan fastEmptyUUO) -- TODO comparing Int writing to (). Change?
        , bench "unagi-chan Unagi.Bounded 1024" $ nfIO (UB.writeChan fastEmptyUBI (0::Int) >> UB.readChan fastEmptyUBO) -- TODO comparing Int writing to (). Change?
        , bench "unagi-chan Unagi.Bounded 1024 with tryWriteChan" $ nfIO (UB.tryWriteChan fastEmptyUBI (0::Int) >> UB.readChan fastEmptyUBO) -- TODO comparing Int writing to (). Change?
        , bench "unagi-chan Unagi.NoBlocking" $ nfIO (UN.writeChan fastEmptyUNI (0::Int) >> tryReadChanErrUN fastEmptyUNO) -- TODO comparing Int writing to (). Change?
        , bench "unagi-chan Unagi.NoBlocking.Unboxed" $ nfIO (UNU.writeChan fastEmptyUNUI (0::Int) >> tryReadChanErrUNU fastEmptyUNUO) -- TODO comparing Int writing to (). Change?
#ifdef COMPARE_BENCHMARKS
        , bench "Chan" $ nfIO $ (writeChan chanEmpty () >> readChan chanEmpty)
        , bench "TQueue" $ nfIO $ (atomically (writeTQueue tqueueEmpty () >>  readTQueue tqueueEmpty))
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
              [ bench "unagi-chan Unagi" $ nfIO $ runtestSplitChanU1 n
              , bench "unagi-chan Unagi.Unboxed" $ nfIO $ runtestSplitChanUU1 n
              , bench "unagi-chan Unagi.Bounded" $ nfIO $ runtestSplitChanUB1 n
              , bench "unagi-chan Unagi.NoBlocking" $ nfIO $ runtestSplitChanUN1 n
              , bench "unagi-chan Unagi.NoBlocking Stream" $ nfIO $ runtestSplitChanUNStream1 n
              , bench "unagi-chan Unagi.NoBlocking.Unboxed" $ nfIO $ runtestSplitChanUNU1 n
              , bench "unagi-chan Unagi.NoBlocking.Unboxed Stream" $ nfIO $ runtestSplitChanUNUStream1 n
#ifdef COMPARE_BENCHMARKS
              , bench "Chan" $ nfIO $ runtestChan1 n
              , bench "TQueue" $ nfIO $ runtestTQueue1 n
           -- , bench "TBQueue" $ runtestTBQueue1 n
           -- , bench "lockfree-queue" $ runtestLockfreeQueue1 n
#endif
              ]
        , bgroup "repeated write some, read some" $ 
              [ bench "unagi-chan Unagi" $ nfIO $ runtestSplitChanU2 n
              , bench "unagi-chan Unagi.Unboxed" $ nfIO $ runtestSplitChanUU2 n
              , bench "unagi-chan Unagi.Bounded" $ nfIO $ runtestSplitChanUB2 n
              , bench "unagi-chan Unagi.NoBlocking" $ nfIO $ runtestSplitChanUN2 n
              , bench "unagi-chan Unagi.NoBlocking Stream" $ nfIO $ runtestSplitChanUNStream2 n
              , bench "unagi-chan Unagi.NoBlocking.Unboxed" $ nfIO $ runtestSplitChanUNU2 n
              , bench "unagi-chan Unagi.NoBlocking.Unboxed Stream" $ nfIO $ runtestSplitChanUNUStream2 n
#ifdef COMPARE_BENCHMARKS
              , bench "Chan" $ nfIO $ runtestChan2 n
              , bench "TQueue" $ nfIO $ runtestTQueue2 n
           -- , bench "TBQueue" $ runtestTBQueue2 n
           -- , bench "lockfree-queue" $ runtestLockfreeQueue2 n
#endif
              ]
        ]
    ]


-- Helper for when we know a read should succeed immediately:
tryReadChanErrUN :: UN.OutChan a -> IO a
{-# INLINE tryReadChanErrUN #-}
tryReadChanErrUN oc = UN.tryReadChan oc 
                    >>= UN.peekElement 
                    >>= maybe (error "A read we expected to succeed failed!") return
tryReadChanErrUNU :: UNU.UnagiPrim a=> UNU.OutChan a -> IO a
{-# INLINE tryReadChanErrUNU #-}
tryReadChanErrUNU oc = UNU.tryReadChan oc 
                    >>= UNU.peekElement 
                    >>= maybe (error "A read we expected to succeed failed!") return



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

-- unagi-chan Unagi.NoBlocking --
runtestSplitChanUN1, runtestSplitChanUN2 :: Int -> IO ()
runtestSplitChanUN1 n = do
  (i,o) <- UN.newChan
  replicateM_ n $ UN.writeChan i ()
  replicateM_ n $ tryReadChanErrUN o

runtestSplitChanUN2 n = do
  (i,o) <- UN.newChan
  let n1000 = n `quot` 1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ UN.writeChan i ()
    replicateM_ n1000 $ tryReadChanErrUN o

-- unagi-chan Unagi.NoBlocking Stream --
runtestSplitChanUNStream1, runtestSplitChanUNStream2 :: Int -> IO ()
runtestSplitChanUNStream1 n = do
  (i,o) <- UN.newChan
  [ oStream ] <- UN.streamChan 1 o
  replicateM_ n $ UN.writeChan i ()
  -- consume until we hit empty:
  let eat str = do
          x <- UN.tryReadStream str
          case x of
               UN.Pending -> return ()
               UN.Cons _ str' -> eat str'
  eat oStream

runtestSplitChanUNStream2 n = do
  (i,o) <- UN.newChan
  [ oStream ] <- UN.streamChan 1 o
  let n1000 = n `quot` 1000
  let eat str = do
          x <- UN.tryReadStream str
          case x of
               UN.Pending -> return str
               UN.Cons _ str' -> eat str'
      writeAndEat iter str = unless (iter <=0) $ do
          replicateM_ n1000 $ UN.writeChan i ()
          eat str >>= writeAndEat (iter-1)
        
  writeAndEat (1000::Int) oStream


-- unagi-chan Unagi.NoBlocking.Unboxed  --
runtestSplitChanUNU1, runtestSplitChanUNU2 :: Int -> IO ()
runtestSplitChanUNU1 n = do
  (i,o) <- UNU.newChan
  replicateM_ n $ UNU.writeChan i (0::Int)
  replicateM_ n $ tryReadChanErrUNU o

runtestSplitChanUNU2 n = do
  (i,o) <- UNU.newChan
  let n1000 = n `quot` 1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ UNU.writeChan i (0::Int)
    replicateM_ n1000 $ tryReadChanErrUNU o

-- unagi-chan Unagi.NoBlocking Stream --
runtestSplitChanUNUStream1, runtestSplitChanUNUStream2 :: Int -> IO ()
runtestSplitChanUNUStream1 n = do
  (i,o) <- UNU.newChan
  [ oStream ] <- UNU.streamChan 1 o
  replicateM_ n $ UNU.writeChan i (0::Int)
  -- consume until we hit empty:
  let eat str = do
          x <- UNU.tryReadStream str
          case x of
               UNU.Pending -> return ()
               UNU.Cons _ str' -> eat str'
  eat oStream

runtestSplitChanUNUStream2 n = do
  (i,o) <- UNU.newChan
  [ oStream ] <- UNU.streamChan 1 o
  let n1000 = n `quot` 1000
  let eat str = do
          x <- UNU.tryReadStream str
          case x of
               UNU.Pending -> return str
               UNU.Cons _ str' -> eat str'
      writeAndEat iter str = unless (iter <=0) $ do
          replicateM_ n1000 $ UNU.writeChan i (0::Int)
          eat str >>= writeAndEat (iter-1)
        
  writeAndEat (1000::Int) oStream



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


-- unagi-chan Unagi Bounded --
-- NOTE: the first does no testing of the bounds checking overhead, while the
-- second does only a little. The multi.hs tests are a better place to look.
runtestSplitChanUB1, runtestSplitChanUB2 :: Int -> IO ()
runtestSplitChanUB1 n = do
  (i,o) <- UB.newChan n
  replicateM_ n $ UB.writeChan i ()
  replicateM_ n $ UB.readChan o

runtestSplitChanUB2 n = do
  let n1000 = n `quot` 1000
  (i,o) <- UB.newChan n1000
  replicateM_ 1000 $ do
    replicateM_ n1000 $ UB.writeChan i ()
    replicateM_ n1000 $ UB.readChan o


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

module Main (main) where

import Control.Monad
import System.Environment
import Control.Concurrent.MVar
import Control.Concurrent
import qualified Control.Concurrent.Chan.Split as S

-- we build this so we can check out what the core looks like.  This is a copy
-- of the "async 2 writers 2 readers" from chan-benchmarks, which has been
-- showing performance regressions.

main = let n = 100000
        in runtestSplitChanAsync 2 2 n

-- NOINLINE ???
runtestSplitChanAsync :: Int -> Int -> Int -> IO ()
runtestSplitChanAsync writers readers n = do
  let nNice = n - rem n (lcm writers readers)
  vs <- replicateM readers newEmptyMVar
  (i,o) <- S.newSplitChan
  replicateM writers $ forkIO $ replicateM_ (nNice `quot` writers) $ S.writeChan i (1 :: Int)
  mapM_ (\v-> forkIO ((replicateM_ (nNice `quot` readers) $ S.readChan o) >> putMVar v ())) vs
  mapM_ takeMVar vs




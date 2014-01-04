module Chan002 where

import Control.Concurrent
import qualified Control.Concurrent.Chan.Split as S
import Control.Exception
import Control.Monad
import System.IO
import System.Environment

-- test for deadlocks
mainChan002 n = do
  replicateM_ n $ do
         (i,o) <- S.newSplitChan
         wid <- forkIO $ forever $ S.writeChan i (5::Int)
         rid <- forkIO $ forever $ void $ S.readChan o
         threadDelay 1000
         throwTo rid ThreadKilled
         putStr "."
         S.readChan o
         throwTo wid ThreadKilled

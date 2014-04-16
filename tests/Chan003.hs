module Chan003 where

import Control.Concurrent
-- import qualified Control.Concurrent.Chan.Split as S
import qualified Control.Concurrent.Chan.Unagi as S
import Control.Exception
import Control.Monad
import System.IO
import System.Environment

-- test for deadlocks
mainChan003 n = do
  replicateM_ n $ do
         (i,_) <- S.newChan
         wid <- forkIO $ forever $ S.writeChan i (5::Int)
         threadDelay 3000
         throwTo wid ThreadKilled
         putStr "."
         S.writeChan i (3::Int)
  putStrLn ""

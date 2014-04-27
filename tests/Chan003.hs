module Chan003 where

import Control.Concurrent
-- import qualified Control.Concurrent.Chan.Split as S
import qualified Control.Concurrent.Chan.Unagi as S
import Control.Exception
import Control.Monad
import System.IO
import System.Environment

-- test for deadlocks from async exceptions raised in writer
checkDeadlocksWriter n = do
  replicateM_ n $ do
         (i,o) <- S.newChan
         wStart <- newEmptyMVar
         wid <- forkIO (putMVar wStart () >> ( forever $ S.writeChan i (0::Int)) )
         -- wait for writer to start
         takeMVar wStart >> threadDelay 1
         throwTo wid ThreadKilled
         -- did killing the writer damage queue for writes or reads?
         S.writeChan i (1::Int)
         z <- S.readChan o
         if z /= 0
            then error "Writer never got a chance to write!"
            else putStr "."
  putStrLn ""

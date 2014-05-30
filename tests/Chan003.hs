module Chan003 where

import Control.Concurrent
import qualified Control.Concurrent.Chan.Unagi as U
import Control.Exception
import Control.Monad

-- TODO we'll have this bind to arguments: newChan readChan writeChan n
--      and pass qualified names as we add tests. Do the same in other generic
--      tests

-- test for deadlocks from async exceptions raised in writer
checkDeadlocksWriter :: Int -> IO ()
checkDeadlocksWriter n = void $
  replicateM_ n $ do
         (i,o) <- U.newChan
         wStart <- newEmptyMVar
         wid <- forkIO (putMVar wStart () >> ( forever $ U.writeChan i (0::Int)) )
         -- wait for writer to start
         takeMVar wStart >> threadDelay 1
         throwTo wid ThreadKilled
         -- did killing the writer damage queue for writes or reads?
         U.writeChan i (1::Int)
         z <- U.readChan o
         unless (z == 0) $
            error "Writer never got a chance to write!"

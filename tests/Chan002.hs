module Chan002 where

import Control.Concurrent
-- import qualified Control.Concurrent.Chan.Split as S
import qualified Control.Concurrent.Chan.Unagi as S
import Control.Exception
import Control.Monad
import System.IO
import System.Environment

-- TODO a variation that kills a blocked reader.

-- test for deadlocks caused by async exceptions in reader.
checkDeadlocksReader times = do
  procs <- getNumCapabilities
  let run 0 = return ()
      run n = do
         (i,o) <- S.newChan
         -- if we don't have at least three cores, then we need to write enough messages in first, before killing reader.
         maybeWid <- if procs > 4 -- NOTE 4 might mean only two real cores, so be conservative here.
                        then do wStart <- newEmptyMVar
                                wid <- forkIO $ (putMVar wStart () >> (forever $ S.writeChan i (0::Int)))
                                takeMVar wStart >> threadDelay 1 -- wait until we're writing
                                return $ Just wid
                             
                             -- TODO wait until writer starts here
                        else do replicateM_ 10000 $ S.writeChan i (0::Int)
                                return Nothing
         rStart <- newEmptyMVar
         rid <- forkIO $ (putMVar rStart () >> (forever $ void $ S.readChan o))
         takeMVar rStart >> threadDelay 1
         throwTo rid ThreadKilled
         -- did killing reader damage queue for reads or writes?
         S.writeChan i 1
         z <- S.readChan o
         -- clean up:
         case maybeWid of 
              Just wid -> throwTo wid ThreadKilled ; _ -> return ()
         if z /= 0
            then putStr "+" >> run n -- reader probably killed while blocked or before writer even wrote anything
            else putStr "." >> run (n-1)
  run times
  putStrLn ""

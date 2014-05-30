module Chan002 where

import Control.Concurrent
import qualified Control.Concurrent.Chan.Unagi as U
import Control.Exception
import Control.Monad


-- TODO we'll have this bind to arguments: newChan readChan writeChan n
--      and pass qualified names as we add tests. Do the same in other generic
--      tests

-- test for deadlocks caused by async exceptions in reader.
checkDeadlocksReader :: Int -> IO ()
checkDeadlocksReader times = do
  -- TODO this also will be an argument, indicating whether a killed reader
  -- might result in one missing element.
  let mightDropOne = True
  procs <- getNumCapabilities
  let run _       0 = putStrLn ""
      run retries n = do
         when (retries > (times `div` 5)) $
            error "This test is taking too long. Please retry, and if still failing send the log to me"
         (i,o) <- U.newChan
         -- if we don't have at least three cores, then we need to write enough messages in first, before killing reader.
         maybeWid <- if procs > 4 -- NOTE 4 might mean only two real cores, so be conservative here.
                        then do wStart <- newEmptyMVar
                                wid <- forkIO $ (putMVar wStart () >> (forever $ U.writeChan i (0::Int)))
                                takeMVar wStart >> threadDelay 1 -- wait until we're writing
                                return $ Just wid
                             
                        else do replicateM_ 10000 $ U.writeChan i (0::Int)
                                return Nothing
         rStart <- newEmptyMVar
         rid <- forkIO $ (putMVar rStart () >> (forever $ void $ U.readChan o))
         takeMVar rStart >> threadDelay 1
         throwTo rid ThreadKilled
         -- did killing reader damage queue for reads or writes?
         U.writeChan i 1 `onException` ( putStrLn "Exception from writeChan 1")
         when mightDropOne $
            U.writeChan i 2 `onException` ( putStrLn "Exception from last writeChan 2")
         z <- U.readChan o `onException` ( putStrLn "Exception from last readChan")
         -- clean up:
         case maybeWid of 
              Just wid -> throwTo wid ThreadKilled ; _ -> return ()
         case z of
              0 -> putStr "." >> run retries (n-1)
              -- reader probably killed while blocked or before writer even wrote anything
              1 -> putStr "+" >> run (retries+1) n 
              2 | not mightDropOne -> error "Fix tests; 2 when not mightDropOne"
                | otherwise -> putStr "*" >> run (retries+1) n
              _ -> error "Fix checkDeadlocksReader test; not 0, 1, or 2"
  run 0 times

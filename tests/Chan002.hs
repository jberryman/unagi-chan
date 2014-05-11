module Chan002 where

import Control.Concurrent
import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Internal as UI
import Control.Exception
import Control.Monad
import System.IO
import System.Environment
import Data.Atomics.Counter

-- TODO This has gotten pretty unagi-specific. probably need a new generic one when we add other chans?
--       - then we could add a simpler generic test that runs forever reader AND writer threads
--          but still check numCapabilities and warn if < 4

-- test for deadlocks caused by async exceptions in reader.
checkDeadlocksReader times = do
  let run 0 normalRetries numRace = putStrLn $ "Lates: "++(show normalRetries)++", Races: "++(show numRace)
      run n normalRetries numRace
       | (normalRetries + numRace) > (times `div` 5) = error "This test is taking too long. Please retry, and if still failing send the log to me"
       | otherwise = do
         -- we'll kill the reader with our special exception half the time,
         -- expecting that we never get our race condition on those runs:
         let usingSpecialKill = even n

         (i,o) <- UI.newChanStarting 0
         -- preload a chan with 0s
         let numPreloaded = 10000
         replicateM_ numPreloaded $ U.writeChan i (0::Int)

         rStart <- newEmptyMVar
         rid <- forkIO $ (if usingSpecialKill then UI.catchKillRethrow else id) $
                    (putMVar rStart () >> (forever $ void $ U.readChan o))
         takeMVar rStart >> threadDelay 1
         if usingSpecialKill
            then UI.throwKillTo rid -- blocks until readChan blocking handler gaurenteed done
            else throwTo rid ThreadKilled
         -- did killing reader damage queue for reads or writes?
         U.writeChan i 1 `onException` ( putStrLn "Exception from first writeChan!")
         U.writeChan i 2 `onException` ( putStrLn "Exception from second writeChan!")
         z <- U.readChan o `onException` ( putStrLn "Exception from last readChan!")
         
         oCnt <- readCounter $ (\(UI.OutChan(UI.ChanEnd _ cntr _))-> cntr) o
         case z of
              0 -> if oCnt <= 0 -- (technically, -1 means hasn't started yet)
                      -- reader didn't have a chance to get started
                     then putStr "0" >> run n (normalRetries+1) numRace
                      -- normal run
                     else putStr "." >> run (n-1) normalRetries numRace
              _ -> do iCnt <- readCounter $ (\(UI.InChan _ (UI.ChanEnd _ cntr _))-> cntr) i
                      if oCnt /= (numPreloaded + 1)
                          then error $ "Checking OutChan counter, expected: "++(show $ numPreloaded + 1)++", but got: "++(show oCnt)
                          else case z of
                             -- reader killed (probably while blocked) after reading all messages
                             1 | iCnt == (numPreloaded + 2) -> putStr "+" >> run n (normalRetries + 1) numRace
                                 -- in rare case we killed the thread just after it read the last message but before it blocked
                               | iCnt == (numPreloaded + 1) -> putStr "#" >> run n (normalRetries + 1) numRace
                               | otherwise -> error $ "Checking InChan counter, expected: "++(show $ numPreloaded + 2)++
                                                      ", or very rarely: "++(show $ numPreloaded + 1)++", but got: "++(show iCnt)
                             -- (rare) exception raised on blocked reader, but write1 raced
                             -- ahead of the exception-handler and its element was lost:
                             2 | usingSpecialKill -> error "We lost a write when using throwKillTo!"
                                 -- we can infer writeChan 1 never retried:
                               | iCnt /= (numPreloaded + 1) -> error $ "With dropped element, Checking InChan counter, expected: "++
                                                                        (show $ numPreloaded + 1)++", but got: "++(show iCnt)
                               | otherwise -> putStr "X" >> run n normalRetries (numRace + 1)
  run times 0 0
  putStrLn ""

module Chan002 where

import Control.Concurrent
import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Internal as UI
import Control.Exception
import Control.Monad
import Data.Atomics.Counter.Fat


-- DIFFERENCES:
--   readChan might fail at any point
--      can't tell much from that.
--   readChanOnException handler (when it's run) should always return a 1 (since only when blocked), and in that scenario finalRead should be 2
--      handler will only have run when finalRead is 2
--      if handler puts to a new MVar we don't have to do our special kill; just block.

-- TODO This has gotten pretty unagi-specific. probably need a new generic one when we add other chans?
--       - then we could add a simpler generic test that runs forever reader AND writer threads
--          but still check numCapabilities and warn if < 4

-- test for deadlocks caused by async exceptions in reader.
checkDeadlocksReader :: Int -> IO ()
checkDeadlocksReader times = do
  let run 0 normalRetries numRace = putStrLn $ "Lates: "++(show normalRetries)++", Races: "++(show numRace)
      run n normalRetries numRace
       | (normalRetries + numRace) > (times `div` 5) = error "This test is taking too long. Please retry, and if still failing send the log to me"
       | otherwise = do
         -- we'll kill the reader with our special exception half the time,
         -- expecting that we never get our race condition on those runs:
         let usingReadChanOnException = even n

         (i,o) <- UI.newChanStarting 0
         -- preload a chan with 0s
         let numPreloaded = 10000
         replicateM_ numPreloaded $ U.writeChan i (0::Int)

         rStart <- newEmptyMVar
         saved <- newEmptyMVar -- for holding potential saved (IO a) from blocked readChanOnException
         rid <- forkIO $ (\rd-> putMVar rStart () >> (forever $ void $ rd o)) $
                    if usingReadChanOnException 
                        then flip U.readChanOnException ( putMVar saved )
                        else U.readChan
         takeMVar rStart >> threadDelay 1
         throwTo rid ThreadKilled

         -- did killing reader damage queue for reads or writes?
         U.writeChan i 1 `onException` ( putStrLn "Exception from first writeChan!")
         U.writeChan i 2 `onException` ( putStrLn "Exception from second writeChan!")
         finalRead <- U.readChan o `onException` ( putStrLn "Exception from final readChan!")
         
         oCnt <- readCounter $ (\(UI.OutChan(UI.ChanEnd _ cntr _))-> cntr) o
         iCnt <- readCounter $ (\(UI.InChan _ (UI.ChanEnd _ cntr _))-> cntr) i
         unless (iCnt == numPreloaded + 1) $ 
            error "The InChan counter doesn't match what we'd expect from numPreloaded!"

         case finalRead of
              0 -> if oCnt <= 0 -- (technically, -1 means hasn't started yet)
                      -- reader didn't have a chance to get started
                     then putStr "0" >> run n (normalRetries+1) numRace
                      -- normal run; we tested that killing a reader didn't
                      -- break chan for other readers/writers:
                     else putStr "." >> run (n-1) normalRetries numRace
              --
              -- Rare. Reader was killed after reading all pre-loaded messages
              -- but before starting what would be the blocking read:
              1 | oCnt == numPreloaded -> putStr "X" >> run n normalRetries (numRace + 1)
                | otherwise -> error $ "Having read final 1, "++
                                       "Expecting a counter value of "++(show numPreloaded)++
                                       " but got: "++(show oCnt)
              2 -> do when usingReadChanOnException $ do
                        shouldBe1 <- join $ takeMVar saved
                        unless (shouldBe1 == 1) $
                          error "The handler for our readChanOnException should only have returned 1"
                      unless (oCnt == numPreloaded + 1) $
                        error $ "Having read final 2, "++
                                "Expecting a counter value of "++(show $ numPreloaded+1)++
                                " but got: "++(show oCnt)
                      putStr "+" >> run n (normalRetries + 1) numRace

              _ -> error "Fix your #$%@ test!"

  run times 0 0
  putStrLn ""

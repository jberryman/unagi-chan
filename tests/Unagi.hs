module Unagi (unagiMain) where

-- Unagi-chan-specific tests

import TestUtils
import Control.Concurrent.Chan.Unagi
import qualified Control.Concurrent.Chan.Unagi.Internal as UI
import Control.Monad
import qualified Data.Primitive as P
import Data.IORef

import Control.Concurrent(forkIO,threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Data.Atomics.Counter.Fat

unagiMain :: IO ()
unagiMain = do
    putStrLn "==================="
    putStrLn "Testing Unagi details:"
    -- ------
    putStr "Smoke test at different starting offsets, spanning overflow... "
    mapM_ smoke $ [ (maxBound - UI.sEGMENT_LENGTH - 1) .. maxBound] 
                  ++ [minBound .. (minBound + UI.sEGMENT_LENGTH + 1)]
    putStrLn "OK"
    -- ------
    putStr "Correct first write... "
    mapM_ correctFirstWrite [ (maxBound - 7), maxBound, minBound, 0]
    putStrLn "OK"
    -- ------
    putStr "Correct initial writes... "
    mapM_ correctInitialWrites [ (maxBound - UI.sEGMENT_LENGTH), (maxBound - UI.sEGMENT_LENGTH) - 1, maxBound, minBound, 0]
    putStrLn "OK"
    -- ------
    let tries = 10000
    putStrLn $ "Checking for deadlocks from killed Unagi reader in a fancy way, x"++show tries
    checkDeadlocksReaderUnagi tries


-- TODO CONSIDER ADDING newChanStarting (or raplcing newChan) TO IMPLEMENTATIONS, AND CONSOLIDATE THESE IN Smoke.hs
smoke :: Int -> IO ()
smoke n = smoke1 n >> smoke2 n

-- www.../rrr... spanning overflow
smoke1 :: Int -> IO ()
smoke1 n = do
    (i,o) <- UI.newChanStarting n
    let inp = [0 .. (UI.sEGMENT_LENGTH * 3)]
    mapM_ (writeChan i) inp
    outp <- getChanContents o
    if and (zipWith (==) inp outp)
        then return ()
        else error $ "Smoke test failed with starting offset of: "++(show n)

-- w/r/w/r... spanning overflow
smoke2 :: Int -> IO ()
smoke2 n = do
    (i,o) <- UI.newChanStarting n
    let inp = [0 .. (UI.sEGMENT_LENGTH * 3)]
    mapM_ (check i o) inp
 where check i o x = do
         writeChan i x
         x' <- readChan o
         if x == x'
            then return ()
            else error $ "Smoke test failed with starting offset of: "++(show n)++"at write: "++(show x)

correctFirstWrite :: Int -> IO ()
correctFirstWrite n = do
    (i,UI.OutChan (UI.ChanEnd _ _ arrRef)) <- UI.newChanStarting n
    writeChan i ()
    (UI.StreamHead _ (UI.Stream arr _)) <- readIORef arrRef
    cell <- P.readArray arr 0
    case cell of
         UI.Written () -> return ()
         _ -> error "Expected a Write at index 0"

-- check writes by doing a segment+1-worth of reads by hand
-- also check that segments pre-allocated as expected
correctInitialWrites :: Int -> IO ()
correctInitialWrites startN = do
    (i,(UI.OutChan (UI.ChanEnd _ _ arrRef))) <- UI.newChanStarting startN
    let writes = [0..UI.sEGMENT_LENGTH]
    mapM_ (writeChan i) writes
    (UI.StreamHead _ (UI.Stream arr next)) <- readIORef arrRef
    -- check all of first segment:
    forM_ (init writes) $ \ix-> do
        cell <- P.readArray arr ix
        case cell of
             UI.Written n
                | n == ix -> return ()
                | otherwise -> error $ "Expected a Write at index "++(show ix)++" of same value but got "++(show n)
             _ -> error $ "Expected a Write at index "++(show ix)
    -- check last write:
    lastSeg  <- readIORef next
    case lastSeg of
         (UI.Next (UI.Stream arr2 next2)) -> do
            cell <- P.readArray arr2 0
            case cell of
                 UI.Written n 
                    | n == last writes -> return ()
                    | otherwise -> error $ "Expected last write at index "++(show $ last writes)++" of same value but got "++(show n)
                 _ -> error "Expected a last Write"
            -- check pre-allocation:
            n2 <- readIORef next2
            case n2 of 
                (UI.Next (UI.Stream _ next3)) -> do
                    n3 <- readIORef next3
                    case n3 of
                        UI.NoSegment -> return ()
                        _ -> error "Too many segments pre-allocated!"
                _ -> error "Next segment was not pre-allocated!"
         _ -> error "No last segment present!"



{- NOTE: we would like this to pass (see trac #9030) but are happy to note that
 -       it fails which somewhat validates the test below

testBlockedRecovery = do
    (i,o) <- newChan
    v <- newEmptyMVar
    rid <- forkIO (putMVar v () >> readChan o)
    takeMVar v
    threadDelay 1000
    throwTo rid ThreadKilled
    -- we race the exception-handler in `readChan` here...
    writeChan i ()
    -- In a buggy implementation, this would consistently win failing by losing
    -- the message and raising BlockedIndefinitely here:
    readChan o
-}


-- test for deadlocks caused by async exceptions in reader.
checkDeadlocksReaderUnagi :: Int -> IO ()
checkDeadlocksReaderUnagi times = do
  let run 0 normalRetries numRace = putStrLn $ "Lates: "++(show normalRetries)++", Races: "++(show numRace)
      run n normalRetries numRace
       | (normalRetries + numRace) > (times `div` 2)  -- NOTE: Unagi now seems to have regular and many more lates than the other tests; I'm not sure why, but lowered the threshold.
           = error_paranoid "This test is taking too long. Please retry, and if still failing send the log to me"
       | otherwise = do
         -- we'll kill the reader with our special exception half the time,
         -- expecting that we never get our race condition on those runs:
         let usingReadChanOnException = even n

         (i,o) <- UI.newChanStarting 0
         -- preload a chan with 0s
         let numPreloaded = 10000
         replicateM_ numPreloaded $ writeChan i (0::Int)

         rStart <- newEmptyMVar
         saved <- newEmptyMVar -- for holding potential saved (IO a) from blocked readChanOnException
         rid <- forkIO $ (\rd-> putMVar rStart () >> (forever $ void $ rd o)) $
                    if usingReadChanOnException 
                        then flip readChanOnException ( putMVar saved )
                        else readChan
         takeMVar rStart >> threadDelay 1
         throwTo rid ThreadKilled

         -- did killing reader damage queue for reads or writes?
         writeChan i 1 `onException` ( putStrLn "Exception from first writeChan!")
         writeChan i 2 `onException` ( putStrLn "Exception from second writeChan!")
         finalRead <- readChan o `onException` ( putStrLn "Exception from final readChan!")
         
         oCnt <- readCounter $ (\(UI.OutChan(UI.ChanEnd _ cntr _))-> cntr) o
         iCnt <- readCounter $ (\(UI.InChan _ (UI.ChanEnd _ cntr _))-> cntr) i
         unless (iCnt == numPreloaded + 2) $ 
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
              1 | oCnt == (numPreloaded+1) -> putStr "X" >> run n normalRetries (numRace + 1)
                | otherwise -> error $ "Having read final 1, "++
                                       "Expecting a counter value of "++(show $ numPreloaded+1)++
                                       " but got: "++(show oCnt)
              2 -> do when usingReadChanOnException $ do
                        shouldBe1 <- join $ takeMVar saved
                        unless (shouldBe1 == 1) $
                          error "The handler for our readChanOnException should only have returned 1"
                      unless (oCnt == numPreloaded + 2) $
                        error $ "Having read final 2, "++
                                "Expecting a counter value of "++(show $ numPreloaded+2)++
                                " but got: "++(show oCnt)
                      putStr "+" >> run n (normalRetries + 1) numRace

              _ -> error "Fix your #$%@ test!"

  run times 0 0
  putStrLn ""

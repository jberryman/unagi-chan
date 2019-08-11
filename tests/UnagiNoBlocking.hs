module UnagiNoBlocking (unagiNoBlockingMain) where

-- Unagi-chan-specific tests

import TestUtils
import Control.Concurrent.Chan.Unagi.NoBlocking
import qualified Control.Concurrent.Chan.Unagi.NoBlocking.Internal as UI
import Control.Monad
import qualified Data.Primitive as P
import Data.IORef
import System.Mem(performGC)
import Data.List(sort)

import Control.Concurrent(forkIO,yield,threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Data.Atomics.Counter.Fat

unagiNoBlockingMain :: IO ()
unagiNoBlockingMain = do
    putStrLn "==================="
    putStrLn "Testing Unagi.NoBlocking details:"
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
    putStr "Checking isActive... "
    replicateM_ 10 isActiveTest
    replicateM_ 10 readChanYieldTest
    putStrLn "OK"
    -- ------
    putStr "Checking streamChan... "
    streamChanSmoke
    replicateM_ 3 $ streamChanConcurrentStreamerReader 10000000
    replicateM_ 3 $ streamChanConcurrentStreamerWriter 10000000
    putStrLn "OK"
    -- ------
    let tries = 10000
    putStrLn $ "Checking for deadlocks from killed Unagi reader in a fancy way, x"++show tries
    checkDeadlocksReaderUnagi tries


-- Helper for when we know a read should succeed immediately:
tryReadChanErr :: OutChan a -> IO a
tryReadChanErr oc = tryReadChan oc 
                    >>= tryRead 
                    >>= maybe (error "A read we expected to succeed failed!") return

-- TODO CONSIDER ADDING newChanStarting (or raplcing newChan) TO IMPLEMENTATIONS, AND CONSOLIDATE THESE IN Smoke.hs
smoke :: Int -> IO ()
smoke n = smoke1 n >> smoke2 n

-- www.../rrr... spanning overflow
smoke1 :: Int -> IO ()
smoke1 n = do
    (i,o) <- UI.newChanStarting n
    let inp = [0 .. (UI.sEGMENT_LENGTH * 3)]
    mapM_ (writeChan i) inp
    forM_ inp $ \inx-> do
        outx <- tryReadChanErr o
        unless (inx == outx) $
            error $ "Smoke test failed with starting offset of: "++(show n)

-- w/r/w/r... spanning overflow
smoke2 :: Int -> IO ()
smoke2 n = do
    (i,o) <- UI.newChanStarting n
    let inp = [0 .. (UI.sEGMENT_LENGTH * 3)]
    mapM_ (check i o) inp
 where check i o x = do
         writeChan i x
         x' <- tryReadChanErr o
         if x == x'
            then return ()
            else error $ "Smoke test failed with starting offset of: "++(show n)++"at write: "++(show x)

correctFirstWrite :: Int -> IO ()
correctFirstWrite n = do
    (i,oc@(UI.OutChan _ (UI.ChanEnd _ _ arrRef))) <- UI.newChanStarting n
    actv <- isActive oc
    unless actv $
        error "not isActive before first and only write"
    writeChan i ()
    (UI.StreamHead _ (UI.Stream arr _)) <- readIORef arrRef
    cell <- P.readArray arr 0
    case cell of
         Just () -> return ()
         _ -> error "Expected a Write at index 0"

-- check writes by doing a segment+1-worth of reads by hand
-- also check that segments pre-allocated as expected
correctInitialWrites :: Int -> IO ()
correctInitialWrites startN = do
    (i,(UI.OutChan _ (UI.ChanEnd _ _ arrRef))) <- UI.newChanStarting startN
    let writes = [0..UI.sEGMENT_LENGTH]
    mapM_ (writeChan i) writes
    (UI.StreamHead _ (UI.Stream arr next)) <- readIORef arrRef
    -- check all of first segment:
    forM_ (init writes) $ \ix-> do
        cell <- P.readArray arr ix
        case cell of
             Just n
                | n == ix -> return ()
                | otherwise -> error $ "Expected a Write at index "++(show ix)++" of same value but got "++(show n)
             _ -> error $ "Expected a Write at index "++(show ix)
    -- check last write:
    lastSeg  <- readIORef next
    case lastSeg of
         (UI.Next (UI.Stream arr2 next2)) -> do
            cell <- P.readArray arr2 0
            case cell of
                 Just n 
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
       | (normalRetries + numRace) > (times `div` 3) = error_paranoid "This test is taking too long. Please retry, and if still failing send the log to me"
       | otherwise = do
         (i,o) <- UI.newChanStarting 0
         -- preload a chan with 0s
         let numPreloaded = 10000
         replicateM_ numPreloaded $ writeChan i (0::Int)

         rStart <- newEmptyMVar
         rid <- forkIO $ (putMVar rStart () >> (forever $ void $ readChan yield o))
         takeMVar rStart >> threadDelay 1
         throwTo rid ThreadKilled

         -- did killing reader damage queue for reads or writes?
         writeChan i 1 `onException` ( putStrLn "Exception from first writeChan!")
         writeChan i 2 `onException` ( putStrLn "Exception from second writeChan!")
         finalRead <- tryReadChanErr o `onException` ( putStrLn "Exception from final tryReadChan!")
         
         oCnt <- readCounter $ (\(UI.OutChan _ (UI.ChanEnd _ cntr _))-> cntr) o
         iCnt <- readCounter $ (\(UI.InChan  _ (UI.ChanEnd _ cntr _))-> cntr) i
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
              1 | oCnt == numPreloaded+1 -> putStr "X" >> run n normalRetries (numRace + 1)
                | otherwise -> error $ "Having read final 1, "++
                                       "Expecting a counter value of "++(show $ numPreloaded+1)++
                                       " but got: "++(show oCnt)
              2 -> do unless (oCnt == numPreloaded + 2) $
                        error $ "Having read final 2, "++
                                "Expecting a counter value of "++(show $ numPreloaded+2)++
                                " but got: "++(show oCnt)
                      putStr "+" >> run n (normalRetries + 1) numRace

              _ -> error "Fix your #$%@ test!"

  run times 0 0
  putStrLn ""

-- do a series of writes, forcing GC making sure remains true, then do the last
-- write and loop on forcing GC until we see False.
isActiveTest :: IO ()
isActiveTest = do
    let n = 100000
        k = n `div` 100
    let testWrites (inc,outc) = replicateM_ 100 $ do
          replicateM_ k $ writeChan inc ()
          performGC
          actv <- isActive outc
          unless actv $
            error "isActive returned False before last write!"

        lastWriteWait iters (inc,outc) = writeChan inc () >> go iters where
          go i | i < (0::Int) = error "Timed out waiting for isActive to return False. Anomaly or possible bug."
               | otherwise = do
                   actv <- isActive outc
                   when actv $ performGC >> go (i-1)
    -- first with newChan:
    c1 <- newChan
    testWrites c1
    lastWriteWait 1000 c1
    -- then with a duplicated channel:
    (inc2,_) <- newChan
    outc2 <- dupChan inc2
    testWrites (inc2,outc2)
    lastWriteWait 1000 (inc2,outc2)

-- Concurrently write [1..100000], while reading 100001 and prepending in a
-- IORef. Then a handler catches and puts () in an MVar which we wait on.
-- Then check that the elements were correct.
readChanYieldTest :: IO ()
readChanYieldTest = do
    let n = 100000 :: Int
    (inc,outc) <- newChan
    saving <- newIORef []
    exceptionRaised <- newIORef False
    goAhead <- newEmptyMVar

    let handling io = Control.Exception.catch io $ \BlockedIndefinitelyOnMVar ->
            writeIORef exceptionRaised True

    void $ forkIO $ replicateM_ (n+1) $ handling $ do
        x <- readChan yield outc
        modifyIORef' saving (x:)
        when (x == n) $ -- about to do final deadlocking loop:
            putMVar goAhead ()
    void $ forkIO $ forM_ [1..n] $ writeChan inc

    takeMVar goAhead
    out <- readIORef saving
    unless (out == [n,n-1..1]) $
        error "readChanYieldTest reads incorrect!"

    performGC
    threadDelay 100000
    raised <- readIORef exceptionRaised
    unless raised $
        error "Handler doesn't seem to have run in readChanYieldTest. Either a testing fluke or a bug."


-- Smoke tests for different strides of interleaved streams, at different
-- offsets, with concurrent stream readers.
streamChanSmoke :: IO ()
streamChanSmoke =
  -- A few odd starting offsets, spanning Int/Counter overflow:
  forM_ [0, maxBound - UI.sEGMENT_LENGTH, maxBound - UI.sEGMENT_LENGTH + 1, maxBound, minBound] $ \startingOffset ->
    -- And few odd numbers of streams, where we especially want to exercise
    -- skips of entire segments: 
    forM_ [1,17, UI.sEGMENT_LENGTH, UI.sEGMENT_LENGTH+1, UI.sEGMENT_LENGTH*3+1] $ \numStreams-> do
      (i,o) <- UI.newChanStarting startingOffset
      let payload = 100000 :: Int
      -- and these are what we'll expect to see returned:
      let payloadPartsRev = map reverse $ [ [s,(s+numStreams).. payload ] | s<-[1..numStreams]]
      forM_ [1..payload] (writeChan i)
      strms <- streamChan numStreams o
      strmsReadOut <- replicateM numStreams newEmptyMVar
      unless (length strms == numStreams) $ 
        error $ "numStreams /= length strms: "
         ++(show numStreams)++" vs "++(show $ length strms)
         ++" at offset: "++(show startingOffset)
      forM_ (zip strms strmsReadOut) (forkIO . consumeUntilEmpty [])
      parts <- forM (zip strmsReadOut payloadPartsRev) $ \(v,expectedStack)-> do
        stack <- takeMVar v
        unless (stack == expectedStack) $ error $ "Incorrect stream reads: "++(show stack)
        return stack
      unless ((sort $ concat parts) == [1..payload]) $
        error $ "Somehow read parts weren't what we expected: "++(show parts)
   where consumeUntilEmpty stack (strm,v) = do
           h <- tryReadNext strm
           case h of
             (Next x xs) -> consumeUntilEmpty (x:stack) (xs,v)
             Pending -> putMVar v stack -- Done

-- Simple writer/streamer concurrency test
streamChanConcurrentStreamerWriter :: Int -> IO ()
streamChanConcurrentStreamerWriter n = do
    (i,o) <- newChan
    [strm] <- streamChan 1 o
    v <- newEmptyMVar
    let streamReader s stack itr failCnt 
          | failCnt > 4 = putMVar v $ Left "failCnt exceeded; possibly bug, but probably anomaly"
          | itr > n = putMVar v $ Right stack
          | otherwise = do
              xs <- tryReadNext s
              case xs of
                Pending -> threadDelay 1000 >> streamReader s stack itr (failCnt+1)
                Next x xs' -> streamReader xs' (x:stack) (itr+1) 0
    void $ forkIO $ streamReader strm [] (1::Int) (0::Int)
    void $ forkIO $ mapM_ (writeChan i) [1..n]
    strmOut <- either error return =<< takeMVar v
    unless (strmOut == [n,n-1..1]) $ 
        error $ "Stream reads were incorrect: "++(show strmOut)

-- Simple reader/streamer concurrency test
streamChanConcurrentStreamerReader :: Int -> IO ()
streamChanConcurrentStreamerReader n = do
    (i,o) <- newChan
    [strm] <- streamChan 1 o
    mapM_ (writeChan i) [1..n]
    vStream <- newEmptyMVar
    vOutchan <- newEmptyMVar
    let streamReader s stack = do
          xs <- tryReadNext s
          case xs of
            Pending -> putMVar vStream stack
            Next x xs' -> streamReader xs' (x:stack)

        outchanReader stack = do
          tryReadChan o >>= tryRead >>= maybe (putMVar vOutchan stack) (outchanReader . (:stack))
    void $ forkIO $ streamReader strm []
    void $ forkIO $ outchanReader []
    strmOut <- takeMVar vStream `onException` putStr " :in takeMVar vStream: "
    rdOut <- takeMVar vOutchan `onException` putStr " :in takeMVar vOutchan: "
    let correctOut = [n,n-1..1]
    unless (strmOut == correctOut) $ 
        error $ "Stream reads were incorrect: "++(show strmOut)
    unless (rdOut == correctOut) $ 
        error $ "OutChan reads were incorrect: "++(show rdOut)


module UnagiBounded(unagiBoundedMain) where

import Control.Concurrent.Chan.Unagi.Bounded
import qualified Control.Concurrent.Chan.Unagi.Bounded.Internal as UI
import Control.Monad
import qualified Data.Primitive as P
import Data.IORef

import Control.Concurrent(forkIO,threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Data.Atomics.Counter.Fat
import Control.Applicative

import Data.Maybe(isNothing)

unagiBoundedMain :: IO ()
unagiBoundedMain = do
    putStrLn "==================="
    putStrLn "Testing Unagi.Bounded details:"
    -- ------
    putStr "Smoke test at different starting offsets, spanning overflow... "
    forM_ [1,2,4,1024,  3,5,513] $ \segLen-> do
        -- ------
        mapM_ (overflowSanity segLen) $ 
            [ (maxBound - segLen - 1) .. maxBound] 
            ++ [minBound .. (minBound + segLen + 1)]
        -- ------
        newChanSanity segLen
    putStrLn "OK"
    -- ------
    putStr "Correct first write and read... "
    mapM_ correctFirstWriteRead [ maxBound - 5, maxBound - 4, maxBound - 3, maxBound, minBound, 0]
    putStrLn "OK"
    -- ------
    let tries = 10000
    putStrLn $ "Checking for deadlocks from killed Unagi reader in a fancy way, x"++show tries
    checkDeadlocksReaderUnagiBounded tries
    -- ------
    putStr "Test bounds blocking... "
    forM_ [(1::Int),2,4,8] $ \n->
        testBoundsBlocking (2^n)
    putStrLn "OK"
    -- ------
    putStrLn "==================="
    putStrLn "Testing Unagi.Bounded components:"
    -- ------
    putStr "Checkpoint tests... "
    checkpointTest1 100000000
    checkpointTest2 100000000
    putStrLn "OK"


-- TODO NOTE WHEN WE FACTOR THIS OUT OF UNAGI*-SPECIFIC TESTS, make function closed over sEGMENT_LENGTH as we do here.
-- NOTE: FORMERLY smoke2
--
-- w/r/w/r... spanning overflow
overflowSanity :: Int -> Int -> IO ()
overflowSanity segLen n = do
    (i,o) <- UI.newChanStarting n segLen
    let inp = [0 .. (segLen * 3)]
    mapM_ (check i o) inp
 where check i o x = do
         writeChan i x
         x' <- readChan o
         unless (x == x') $
            error $ "Smoke test failed with starting offset of: "++(show n)++"at write: "++(show x)

-- exercise power-of-two rounding, and make sure array sizes and recorded
-- bounds match up
newChanSanity :: Int -> IO ()
newChanSanity bnds = do
    (UI.InChan _ inCE, UI.OutChan outCE) <- newChan bnds
    check inCE
    check outCE
    checkSame inCE outCE
  where checkSame (UI.ChanEnd x y _ _ _) (UI.ChanEnd x' y' _ _ _) =
           unless (x == x' && y == y') $
             error $ "newChanSanity: "++(show (x,x',y,y'))

        check (UI.ChanEnd logBounds boundsMn1 _segSource _ _) = do
           let storedBoundsSane = (2^logBounds) == (boundsMn1 + 1)
           lengthEqualsBounds <- return True -- TODO get and check length from segSource arr, possible?
           unless (storedBoundsSane && lengthEqualsBounds) $
            error $ "newChanSanity: PLORT!"


-- Basic first write sanity checking and writer unblocking mechanics
correctFirstWriteRead :: Int -> IO ()
correctFirstWriteRead n = do
    let size = 4
    (i, o@(UI.OutChan (UI.ChanEnd _logBounds _boundsMn1 _segSource _cntr strHeadRef))) <- UI.newChanStarting n size

    writeChan i ()

    (UI.StreamHead offset0 (UI.Stream arr next)) <- readIORef strHeadRef
    cell <- P.readArray arr 0
    case cell of
         UI.Written () -> return ()
         _ -> error "Expected a Write at index 0"
    unless (n == offset0)$
        error $ "offset0 /= "++(show n)++", instead == "++(show offset0)

    -- The next segment should not be set up yet:
    noSegment <- readIORef next
    unless (isNothing noSegment) $
        error "Next segment should not be set up yet!"

    -- First read should set up next segment for unblocked writers.
    () <- readChan o `onException` putStrLn "Read of first and only value failed!"
    nextSeg <- readIORef next
    case nextSeg of
         Nothing -> error "Next should have been set up after first read!"
         Just (UI.NextByReader (UI.Stream _arr' next')) -> do
            noSegment' <- readIORef next'
            unless (isNothing noSegment') $
                error "Next of next segment should not be set up yet!"
         _ -> error "Next marked installed by writer!"



-- test for deadlocks caused by async exceptions in reader.
checkDeadlocksReaderUnagiBounded :: Int -> IO ()
checkDeadlocksReaderUnagiBounded times = do
  let run 0 normalRetries numRace = putStrLn $ "Lates: "++(show normalRetries)++", Races: "++(show numRace)
      run n normalRetries numRace
       | (normalRetries + numRace) > (times `div` 3) = error "This test is taking too long. Please retry, and if still failing send the log to me"
       | otherwise = do
         -- we'll kill the reader with our special exception half the time,
         -- expecting that we never get our race condition on those runs:
         let usingReadChanOnException = even n

         let numPreloaded = 10000
         (i,o) <- UI.newChanStarting 0 $ numPreloaded+2
         -- preload a chan with 0s
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
         
         oCnt <- readCounter $ (\(UI.OutChan(UI.ChanEnd _ _ _ cntr _))-> cntr) o
         iCnt <- readCounter $ (\(UI.InChan _ (UI.ChanEnd _ _ _ cntr _))-> cntr) i
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

-- idempotent & non-conflicting puts/takes
checkpointTest1 :: Int -> IO ()
checkpointTest1 n = do
    x <- newEmptyMVar
    checkpt <- UI.WriterCheckpoint <$> newEmptyMVar
    void $ forkIO ((replicateM_ n $ UI.writerCheckin checkpt) >> putMVar x ())
    threadDelay 10000
    replicateM_ n $ UI.unblockWriters checkpt
    takeMVar x `onException` putStrLn "checkpointTest1: Forked writerCheckin deadlocked!"

-- No deadlocks in read:
checkpointTest2 :: Int -> IO ()
checkpointTest2 n = do
    x <- newEmptyMVar
    checkpt <- UI.WriterCheckpoint <$> newEmptyMVar
    void $ forkIO ((replicateM_ n $ UI.writerCheckin checkpt) >> putMVar x ())
    threadDelay 10000
    UI.unblockWriters checkpt
    replicateM_ n $ UI.writerCheckin checkpt
    takeMVar x `onException` putStrLn "checkpointTest2: Forked writerCheckin deadlocked!"


-- Test that our bounding works as expected
testBoundsBlocking :: Int -> IO ()
testBoundsBlocking bnds = do
    v <- newEmptyMVar
    (inC,outC) <- newChan bnds
    -- make sure none of these block.
    replicateM_ bnds $ writeChan inC ()
    -- but this blocks
    void $ forkIO $ writeChan inC () >> putMVar v ()
    threadDelay 100000
    noth <- tryTakeMVar v
    unless (noth == Nothing) $
        error "bnds+1 writeChan should have blocked!"
    -- ...until:
    readChan outC
    takeMVar v `onException` putStrLn "Read should have unblocked bnds+1 writer"
    -- Now we fork one which work together to fill remaining bnds-1 slots, without blocking:
    (replicateM_ (bnds-1) $ writeChan inC ())
        `onException` putStrLn "Writes should not have blocked in second segment"
    -- now we're at 2x bounds.
    -- This next, again, should block:
    v2 <- newEmptyMVar 
    void $ forkIO $ writeChan inC () >> putMVar v2 ()
    threadDelay 100000
    noth2 <- tryTakeMVar v2
    unless (noth2 == Nothing) $
        error "bnds*2+1 writeChan should have blocked!"
    -- and unblock as soon as (but no sooner than after bnds reads):
    replicateM_ (bnds-1) $ readChan outC
    threadDelay 100000
    noth3 <- tryTakeMVar v2
    unless (noth3 == Nothing) $
        error "bnds*2+1 writeChan should still have been blocked!"
    -- now this should unblock:
    readChan outC
    takeMVar v2 `onException` putStrLn "Read should have unblocked bnds*2+1 writer"

    

module Atomics (atomicsMain) where

import Control.Concurrent
import Data.Atomics.Counter.Fat
import Data.Atomics
import Data.IORef
import Control.Monad
import qualified Data.Set as Set
import Data.List
import Data.Bits

atomicsMain :: IO ()
atomicsMain = do
    putStrLn "Testing atomic-primops:"
    -- ------
    putStr "    counter smoke test... "
    counterSane
    putStrLn "OK"
    -- ------
    putStr "    counter overflow... "
    testCounterOverflow
    putStrLn "OK"
    -- ------
    putStr "    counter is atomic... "
    counterTest
    putStrLn "OK"
    -- ------
    putStr "    CAS... "
    testConsistentSuccessFailure
    putStrLn "OK"

-- catch real stupid bugs before machine gets hot:
counterSane :: IO ()
counterSane = do
    cntr <- newCounter 1337
    n <- readCounter cntr
    unless (n == 1337) $ error "newCounter magnificently broken"
    n2 <- incrCounter 1 cntr
    n2' <- readCounter cntr
    unless (n2 == 1338 && n2' == 1338) $ error "incrCounter magnificently broken"

cHUNK_SIZE, maxInt, minInt :: Int
cHUNK_SIZE = 32 -- MUST REMAIN POWER OF TWO for now
-- make sure incrCounter doesn't silently change to Integer or something
maxInt = maxBound
minInt = minBound

-- Test some properties of our counter we'd like to assume:
testCounterOverflow :: IO ()
testCounterOverflow = do
    let x `ourMod` y = 
           -- also do a little sanity checking for the bitwise mod over Int
           -- that we use heavily, and expect in some cases to roll over w/out
           -- breaks:
           let xmy = x .&. (y-1)
            in if xmy == x `mod` y
                    then xmy
                    else error "Our bitwise mod isn't working the way we expect in testCounterOverflow"
    cntr <- newCounter (maxInt - (cHUNK_SIZE `div` 2)) 
    spanningCntr <- replicateM cHUNK_SIZE (incrCounter 1 cntr)
    -- make sure our test is working
    if all (>0) spanningCntr || all (<0) spanningCntr
        then error "Sequence meant to span maxBound of counter not actually spanning"
        else return ()

    let l = map (`ourMod` cHUNK_SIZE) spanningCntr
        l' = (dropWhile (/= 0) l) ++ (takeWhile (/= 0) l)

    -- (1) test that we overflow the counter without any breaks and our mod function is working properly:
    unless (l' == [0..(cHUNK_SIZE - 1)]) $ 
        error $ "Uh Oh: "++(show l')

    -- (2) test that Ints and counter overflow in exactly the same way
    let spanningInts = take cHUNK_SIZE $ iterate (+1) (maxInt - (cHUNK_SIZE `div` 2) + 1) 
    unless (spanningInts == spanningCntr) $ do
        error $ "Ints overflow differently than counter: "++
                "\nInt: "++(show spanningInts)++
                "\nCounter: "++(show spanningCntr)

    -- (We don't use this property)
    cntr2 <- newCounter maxInt
    mbnd <- incrCounter 1 cntr2
    unless (mbnd == minInt) $ 
        error $ "Incrementing counter at maxbound didn't yield minBound"

    -- (3) test subtraction across boundary: count - newFirstIndex, for window spanning boundary.
    cntr3 <- newCounter (maxInt - 1)
    let ls = take 30 $ iterate (+1) $ maxInt - 10
    cs <- mapM (\x-> fmap (subtract x) $ incrCounter 1 cntr3) ls
    unless (cs == replicate 30 10) $ 
        error $ "Derp. We don't know how subtraction works: "++(show cs)

-- Test these assumptions:
--   1) If a CAS fails in thread 1 then another CAS (in thread 2, say) succeeded; i.e. no false negatives
--   2) In the case that thread 1's CAS failed, the ticket returned with (False,tk) will contain that newly-written value from thread 2
testConsistentSuccessFailure :: IO ()
testConsistentSuccessFailure = do
    var <- newIORef "0"

    sem <- newIORef (0::Int)
    outs <- replicateM 2 newEmptyMVar 

    _ <- forkSync sem 2 $ test "a" var (outs!!0)
    _ <- forkSync sem 2 $ test "b" var (outs!!1)

    mapM takeMVar outs >>= examine
       -- w/r/t (2) above: we only try to find an element read along with False
       -- which wasn't sent by another thread, which isn't ideal
 where attempts = 100000
       test tag var out = do
         
         res <- forM [(1::Int)..attempts] $ \x-> do
                    let str = (tag++(show x))
                    tk <- readForCAS var
                    (b,tk') <- casIORef var tk str
                    return (if b then str else peekTicket tk' , b)
         putMVar out res

       examine [res1, res2] = do
         -- any failures in either should be marked as successes in the other
         let (successes1,failures1) = (\(x,y)-> (Set.fromList $ map fst x, map fst y)) $ partition snd res1
             (successes2,failures2) = (\(x,y)-> (Set.fromList $ map fst x, map fst y)) $ partition snd res2
             ok1 = all (flip Set.member successes2) failures1
             ok2 = all (flip Set.member successes1) failures2
         if ok1 && ok2
             then when (length failures1 < (attempts `div` 6) || length failures2 < (attempts `div` 6)) $
                    error "There was not enough contention to trust test. Please retry."
             else do print res1
                     print res2
                     error "FAILURE!"
       examine _ = error "Fix testConsistentSuccessFailure"

                   
forkSync :: IORef Int -> Int -> IO () -> IO ThreadId
forkSync sem target io = 
    forkIO $ (busyWait >> io)
  where busyWait =
           atomicModifyIORef' sem (\n-> (n+1,())) >> wait
        wait = do
            n <- readIORef sem
            unless (n == target) wait
    

counterTest :: IO ()
counterTest = do
    let n = 10000000
    nOut <- testAtomicCounter n
    when (nOut /= n) $
        error $ "Counter broken: expecting "++(show n)++" got "++(show nOut)

testAtomicCounter :: Int -> IO Int
testAtomicCounter n = do
  procs <- getNumCapabilities

  counter <- newCounter (0::Int)
  dones <- replicateM procs newEmptyMVar ; starts <- replicateM procs newEmptyMVar
  mapM_ (\(start1,done1)-> forkIO $ takeMVar start1 >> replicateM_ (n `div` procs) (incrCounter 1 counter) >> putMVar done1 ()) $ zip starts dones
  mapM_ (\v-> putMVar v ()) starts ; mapM_ (\v-> takeMVar v) dones
  
  readCounter counter

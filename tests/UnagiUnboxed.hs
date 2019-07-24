{-# LANGUAGE RankNTypes , ScopedTypeVariables , BangPatterns #-}
module UnagiUnboxed (unagiUnboxedMain) where

-- Unagi-chan-specific tests
--
-- Forked from tests/Unagi.hs at 337600f

import Control.Concurrent.Chan.Unagi.Unboxed
import qualified Control.Concurrent.Chan.Unagi.Unboxed.Internal as UI
import Control.Monad
import qualified Data.Primitive as P
import Data.Primitive.Ptr
import Data.IORef

import Data.Int(Int8,Int16,Int32,Int64)
import Data.Word
import Data.Maybe
import Data.Typeable

import Control.Concurrent(forkIO,threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Data.Atomics.Counter.Fat

import Prelude

unagiUnboxedMain :: IO ()
unagiUnboxedMain = do
    putStrLn "==================="
    putStrLn "Testing Unagi.Unboxed details:"
    -- ------
    putStr "Smoke test at different starting offsets, spanning overflow... "
    mapM_ smoke $ [ (maxBound - UI.sEGMENT_LENGTH - 1) .. maxBound]
                  ++ [minBound .. (minBound + UI.sEGMENT_LENGTH + 1)]
    putStrLn "OK"
    -- ------
    putStr "segSource sanity... "
    applyToAllPrim segSourceMagicSanity
    putStrLn "OK"
    -- ------
    putStr "Atomicity of atomic unicorns... "
    applyToAllPrim atomicUnicornAtomicicity
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


smoke :: Int -> IO ()
smoke n = do
    smoke1 n
    smoke2 n
    -- test each of UnagiPrim
    applyToAllPrim smokeManyElement
    -- test for atomicUnicorns
    applyToAllPrim smokeManyUnicorn

-- test a function against each Prim type elements not equal to atomicUnicorn
applyToAllPrim :: (forall e. (Num e, Typeable e, Show e, UnagiPrim e)=> e -> IO ()) -> IO ()
applyToAllPrim f = do
    f ('c' :: Char)
    f (3.14159 :: Float)
    f (-1.000000000000001  :: Double)
    f (maxBound :: Int)
    f (minBound :: Int8)
    f (maxBound :: Int16)
    f (minBound :: Int32)
    f (maxBound :: Int64)
    f (maxBound :: Word)
    f (maxBound :: Word8)
    f (minBound :: Word16)
    f (minBound :: Word32)
    f (minBound :: Word64)
    f (nullPtr `advancePtr` 1024 :: (Ptr Word))

-- TODO Maybe refactor & get rid of these

instance Num (Ptr a) where

instance Num Char where


-- www.../rrr... spanning overflow
smoke1 :: Int -> IO ()
smoke1 n = do
    (i,o) <- UI.newChanStarting n
    let inp = [0 .. (UI.sEGMENT_LENGTH * 3)]
    mapM_ (writeChan i) inp
    outp <- getChanContents o
    unless (and (zipWith (==) inp outp)) $
        error $ "Smoke test failed with starting offset of: "++(show n)

-- w/r/w/r... spanning overflow
smoke2 :: Int -> IO ()
smoke2 n = do
    (i,o) <- UI.newChanStarting n
    let inp = [0 .. (UI.sEGMENT_LENGTH * 3)]
    mapM_ (check i o) inp
 where check i o x = do
         writeChan i x
         x' <- readChan o
         unless (x == x') $
            error $ "Smoke test failed with starting offset of: "++(show n)++"at write: "++(show x)

-- for smoke checking size, and alignment of different Prim types, and allowing
-- testing of writing atomicUnicorn
smokeManyElement :: (Num e, Typeable e, Show e, UnagiPrim e)=> e -> IO ()
smokeManyElement e = do
    (i,o) <- newChan
    let n = UI.sEGMENT_LENGTH*2 + 1
    replicateM_ n (writeChan i e)
    outp <- getChanContents o
    unless (all (== e) $ take n outp) $
        error $ "smokeManyElement failed with type "++(show $ typeOf e)++": "++(show e)++"  /=  "++(show outp)

-- smokeManyElement for atomicUnicorn values
smokeManyUnicorn :: forall e. (Num e, Typeable e, Show e, UnagiPrim e)=> e -> IO ()
smokeManyUnicorn _ =
    maybe (return ()) smokeManyElement (atomicUnicorn :: Maybe e)

-- check our segSource is doing what we expect with magic values:
segSourceMagicSanity :: forall e. (Num e, Typeable e, Show e, UnagiPrim e)=> e -> IO ()
segSourceMagicSanity _ =
    case atomicUnicorn :: Maybe e of
      Nothing -> return ()
      Just e -> do
        (_,eArr) <- UI.segSource :: IO (UI.SignalIntArray, UI.ElementArray e)
        forM_ [0.. UI.sEGMENT_LENGTH-1] $ \i-> do
           e' <- UI.readElementArray eArr i
           unless (e == e') $
              error $ "in segSource, with type "++(show $ typeOf e)++", "++(show e)++" /= "++(show e')

-- -------------

-- Make sure we get no tearing of adjacent word-size or smaller (as determined
-- by atomicUnicorn instantiation) values, making sure we cross a cache-line.
atomicUnicornAtomicicity :: forall e. (Num e, Typeable e, Show e, UnagiPrim e)=> e -> IO ()
atomicUnicornAtomicicity _e =
  when (isJust  (atomicUnicorn :: Maybe e)) $ do
    (_,eArr) <- UI.segSource :: IO (UI.SignalIntArray, UI.ElementArray e)
    let iters = (64 `quot` P.sizeOf _e) + 1
    when (iters >= UI.sEGMENT_LENGTH) $
      error "Our sEGMENT_LENGTH is smaller than expected; please fix test"
    -- just skip Addr for now TODO:
    unless (  isJust (cast _e :: Maybe (Ptr Word))
           || isJust (cast _e :: Maybe Char)) $
      forM_ [0.. iters] $ \i0 -> do
        let i1 = i0+1
            rd = UI.readElementArray eArr
        first0 <- rd i0
        first1 <- rd i1
        v0 <- newEmptyMVar
        v1 <- newEmptyMVar
        let incr f v i = go
             where go _ 0 = putMVar v ()
                   go expected n = do
                     val <- rd i
                     unless (val == expected) $
                       error $ "atomicUnicornAtomicicity with type "++(show $ typeOf val)++" "++(show val)++" /= "++(show expected)
                     let !next = f val
                     UI.writeElementArray eArr i next
                     go next (n-1)
        let counts = 1000000 :: Int
        _ <- forkIO $ incr (+1) v0 i0 first0 counts
        _ <- forkIO $ incr (subtract 1) v1 i1 first1 counts
        -- BlockedIndefinitelyOnMVar means a problem TODO make better
        takeMVar v0 >> takeMVar v1


correctFirstWrite :: Int -> IO ()
correctFirstWrite n = do
    (i,UI.OutChan (UI.ChanEnd _ arrRef)) <- UI.newChanStarting n
    writeChan i (7::Int)
    (UI.StreamHead _ (UI.Stream sigArr eArr _ _)) <- readIORef arrRef
    cell <- UI.readElementArray eArr 0 :: IO Int
    case cell of
         7 -> return ()
         _ -> error "Expected a write at index 0 of 7"
    sigCell <- P.readByteArray sigArr 0 :: IO Int
    unless (sigCell == 1) $ -- i.e. cellWritten
         error "Expected cellWritten at sigArr!0"

-- check writes by doing a segment+1-worth of reads by hand
-- also check that segments pre-allocated as expected
correctInitialWrites :: Int -> IO ()
correctInitialWrites startN = do
    (i,(UI.OutChan (UI.ChanEnd _ arrRef))) <- UI.newChanStarting startN
    let writes = [0..UI.sEGMENT_LENGTH]
    mapM_ (writeChan i) writes
    (UI.StreamHead _ (UI.Stream sigArr eArr _ next)) <- readIORef arrRef
    -- check all of first segment:
    forM_ (init writes) $ \ix-> do
        cell <- UI.readElementArray eArr ix :: IO Int
        sigCell <- P.readByteArray sigArr ix :: IO Int
        unless (cell == ix && sigCell == 1) $
            error $ "Expected a write at index "++(show ix)++" of same value but got "++(show cell)++" with signal "++(show sigCell)
    -- check last write:
    lastSeg  <- readIORef next
    case lastSeg of
         (UI.Next (UI.Stream sigArr2 eArr2 _ next2)) -> do
            cell <- UI.readElementArray eArr2 0 :: IO Int
            sigCell <- P.readByteArray sigArr2 0 :: IO Int
            unless (cell == last writes && sigCell == 1) $
                error $ "Expected last write at index "++(show $ last writes)++" of same value but got "++(show cell)
            -- check pre-allocation:
            n2 <- readIORef next2
            case n2 of
                (UI.Next (UI.Stream _ _ _ next3)) -> do
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
       | (normalRetries + numRace) > (times `div` 3) = error "This test is taking too long. Please retry, and if still failing send the log to me"
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

         oCnt <- readCounter $ (\(UI.OutChan(UI.ChanEnd cntr _))-> cntr) o
         iCnt <- readCounter $ (\(UI.InChan (UI.ChanEnd cntr _))-> cntr) i
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

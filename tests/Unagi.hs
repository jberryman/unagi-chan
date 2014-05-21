module Unagi (unagiMain) where

-- Unagi-chan-specific tests

import Control.Concurrent.Chan.Unagi
import qualified Control.Concurrent.Chan.Unagi.Internal as UI
import Control.Monad
import Control.Exception
import Control.Concurrent.MVar
import Control.Concurrent(forkIO,threadDelay)
import qualified Data.Primitive as P
import Data.IORef

unagiMain :: IO ()
unagiMain = do
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


smoke n = smoke1 n >> smoke2 n

-- www.../rrr... spanning overflow
smoke1 n = do
    (i,o) <- UI.newChanStarting n
    let inp = [0 .. (UI.sEGMENT_LENGTH * 3)]
    mapM_ (writeChan i) inp
    outp <- getChanContents o
    if and (zipWith (==) inp outp)
        then return ()
        else error $ "Smoke test failed with starting offset of: "++(show n)

-- w/r/w/r... spanning overflow
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
    (i,o@(UI.OutChan (UI.ChanEnd _ _ arrRef))) <- UI.newChanStarting startN
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

module Unagi (unagiMain) where

-- Unagi-chan-specific tests

import Control.Concurrent.Chan.Unagi
import Control.Concurrent.Chan.Unagi.Internal(newChanStarting,sEGMENT_LENGTH)
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
    mapM_ smoke $ [ (maxBound - sEGMENT_LENGTH - 1) .. maxBound] 
                  ++ [minBound .. (minBound + sEGMENT_LENGTH + 1)]
    putStrLn "OK"
    -- ------
    putStr "Testing special async exception handling in blocked reader... "
    replicateM_ 100 $ testBlockedRecovery
    putStrLn "OK"
    -- ------
    putStr "Correct first write... "
    mapM_ correctFirstWrite [ (maxBound - 7), maxBound, minBound, 0]
    putStrLn "OK"


smoke n = smoke1 n >> smoke2 n

-- write all / read all spanning overflow
smoke1 n = do
    (i,o) <- newChanStarting n
    let inp = [0 .. (sEGMENT_LENGTH * 3)]
    mapM_ (writeChan i) inp
    outp <- getChanContents o
    if and (zipWith (==) inp outp)
        then return ()
        else error $ "Smoke test failed with starting offset of: "++(show n)

-- w/r/w/r... spanning overflow
smoke2 n = do
    (i,o) <- newChanStarting n
    let inp = [0 .. (sEGMENT_LENGTH * 3)]
    mapM_ (check i o) inp
 where check i o x = do
         writeChan i x
         x' <- readChan o
         if x == x'
            then return ()
            else error $ "Smoke test failed with starting offset of: "++(show n)++"at write: "++(show x)

correctFirstWrite :: Int -> IO ()
correctFirstWrite n = do
    (i,UI.OutChan (UI.ChanEnd _ _ arrRef)) <- newChanStarting n
    writeChan i ()
    (UI.Stream _ arr _) <- readIORef arrRef
    cell <- P.readArray arr 0
    case cell of
         UI.Written () -> return ()
         _ -> error "Expected a Write at index 0"

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
-- THIS IS A BIT REDUNDANT (see Chan002), but keep for now.
-- we make sure that we can't observe a message disappearing when we kill a
-- blocked reader with our special throw function and then do a write.
testBlockedRecovery = do
    (i,o) <- newChan
    v <- newEmptyMVar
    rid <- forkIO $ UI.catchKillRethrow $ (putMVar v () >> readChan o)
    takeMVar v
    threadDelay 1000
    UI.throwKillTo rid
    -- we race the exception-handler in `readChan` here...
    writeChan i ()
    -- In a buggy implementation, this would consistently win failing by losing
    -- the message and raising BlockedIndefinitely here:
    readChan o

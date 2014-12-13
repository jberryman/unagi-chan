module Deadlocks (deadlocksMain) where

import Control.Concurrent.MVar
import Control.Concurrent(getNumCapabilities,threadDelay,forkIO)
import Control.Exception
import Control.Monad

import Implementations
import qualified Control.Concurrent.Chan.Unagi.Bounded as UB


deadlocksMain :: IO ()
deadlocksMain = do
    let tries = 10000

    putStrLn "==================="
    putStrLn "Testing Unagi:"
    -- ------
    putStr $ "    Checking for deadlocks from killed reader, x"++show tries++"... "
    checkDeadlocksReader unagiImpl tries
    putStrLn "OK"
    -- ------
    putStr $ "    Checking for deadlocks from killed writer, x"++show tries++"... "
    checkDeadlocksWriter unagiImpl tries
    putStrLn "OK"

    putStrLn "==================="
    putStrLn "Testing Unagi.NoBlocking:"
    -- ------
    putStr $ "    Checking for deadlocks from killed reader, x"++show tries++"... "
    checkDeadlocksReader unagiNoBlockingImpl tries
    putStrLn "OK"
    -- ------
    putStr $ "    Checking for deadlocks from killed writer, x"++show tries++"... "
    checkDeadlocksWriter unagiNoBlockingImpl tries
    putStrLn "OK"
    
    putStrLn "==================="
    putStrLn "Testing Unagi.NoBlocking.Unboxed:"
    -- ------
    putStr $ "    Checking for deadlocks from killed reader, x"++show tries++"... "
    checkDeadlocksReader unagiNoBlockingUnboxedImpl tries
    putStrLn "OK"
    -- ------
    putStr $ "    Checking for deadlocks from killed writer, x"++show tries++"... "
    checkDeadlocksWriter unagiNoBlockingUnboxedImpl tries
    putStrLn "OK"
    
    putStrLn "==================="
    putStrLn "Testing Unagi.Unboxed:"
    -- ------
    putStr $ "    Checking for deadlocks from killed reader, x"++show tries++"... "
    checkDeadlocksReader unboxedUnagiImpl tries
    putStrLn "OK"
    -- ------
    putStr $ "    Checking for deadlocks from killed writer, x"++show tries++"... "
    checkDeadlocksWriter unboxedUnagiImpl tries
    putStrLn "OK"

    putStrLn "==================="
    putStrLn "Testing Unagi.Bounded:"
    -- ------
    putStr $ "    Checking for deadlocks from killed reader, x"++show tries++"... "
    -- bounds must be > 10000 here (note actual bounds rounded up to power of 2):
    checkDeadlocksReader (unagiBoundedImpl 50000) tries
    putStrLn "OK"
    -- ------
    putStr $ "    Checking for deadlocks from killed writer, x"++show tries++"... "
    -- fragile bounds must be large enought to never be reached here:
    checkDeadlocksWriterBounded tries
    putStrLn "OK"


-- -- Chan002.hs -- --



-- test for deadlocks caused by async exceptions in reader.
checkDeadlocksReader :: Implementation inc outc Int -> Int -> IO ()
checkDeadlocksReader (newChan,writeChan,readChan,_) times = do
  -- this might become an argument, indicating whether a killed reader might
  -- result in one missing element (currently all do)
  let mightDropOne = True
  procs <- getNumCapabilities
  let run _       0 = putStrLn ""
      run retries n = do
         when (retries > (times `div` 3)) $
            error "This test is taking too long. Please retry, and if still failing send the log to me"
         (i,o) <- newChan
         -- if we don't have at least three cores, then we need to write enough messages in first, before killing reader.
         maybeWid <- if procs > 4 -- NOTE 4 might mean only two real cores, so be conservative here.
                        then do wStart <- newEmptyMVar
                                wid <- forkIO $ (putMVar wStart () >> (forever $ writeChan i (0::Int)))
                                takeMVar wStart >> threadDelay 1 -- wait until we're writing
                                return $ Just wid
                             
                        else do replicateM_ 15000 $ writeChan i (0::Int)
                                return Nothing
         rStart <- newEmptyMVar
         rid <- forkIO $ (putMVar rStart () >> (forever $ void $ readChan o))
         takeMVar rStart >> threadDelay 1
         throwTo rid ThreadKilled
         -- did killing reader damage queue for reads or writes?
         writeChan i 1 `onException` ( putStrLn "Exception from writeChan 1")
         when mightDropOne $
            writeChan i 2 `onException` ( putStrLn "Exception from last writeChan 2")
         z <- readChan o `onException` ( putStrLn "Exception from last readChan")
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


-- -- Chan003.hs -- --


-- test for deadlocks from async exceptions raised in writer
checkDeadlocksWriter :: Implementation inc outc Int -> Int -> IO ()
checkDeadlocksWriter (newChan,writeChan,readChan,_) n = void $
  replicateM_ n $ do
         (i,o) <- newChan
         wStart <- newEmptyMVar
         wid <- forkIO (putMVar wStart () >> ( forever $ writeChan i (0::Int)) )
         -- wait for writer to start
         takeMVar wStart >> threadDelay 1
         throwTo wid ThreadKilled
         -- did killing the writer damage queue for writes or reads?
         writeChan i (1::Int)
         z <- readChan o
         unless (z == 0) $
            error "Writer never got a chance to write!"

-- A bit ugly, but we need this slight variant for Bounded variant:
checkDeadlocksWriterBounded :: Int -> IO ()
checkDeadlocksWriterBounded cnt = go 0 cnt where
  go lates n 
    | lates > (cnt `div` 4) = error "This is taking too long; we probably need a bigger bounds, sorry." 
    | otherwise = 
       when (n > 0) $ do
         (i,o) <- UB.newChan (2^(14::Int))
         wStart <- newEmptyMVar
         wid <- forkIO (putMVar wStart () >> ( forever $ UB.writeChan i (0::Int)) )
         -- wait for writer to start
         takeMVar wStart >> threadDelay 1
         throwTo wid ThreadKilled
         -- did killing the writer damage queue for writes or reads?
         success <- UB.tryWriteChan i (1::Int)
         if success
             then do
                 z <- UB.readChan o
                 if (z /= 0)
                    -- Writer never got a chance to write, retry:
                    then go (lates+1) n
                    -- OK:
                    else go lates (n-1)

             -- throwTo probably didn't catch writeChan while running, retry:
             else go (lates+1) n


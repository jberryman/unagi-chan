module Qsem001 where

import Control.Concurrent.QSem as OldQ

import Control.Concurrent.Chan.Unagi
-- import Control.Concurrent.Chan.Split
import Control.Concurrent (forkIO, threadDelay, killThread, yield)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad

new = newQSem
wait = waitQSem
signal = signalQSem

-- --------------------------
-- NOTE: these are tests for QSem, but we'd like to be sure our Chan doesn't
-- break them.
-- --------------------------

--------
-- dummy test-framework

type Assertion = IO ()

x @?= y = when (x /= y) $ error (show x ++ " /= " ++ show y)

testCase :: String -> IO () -> IO ()
testCase n io = putStrLn ("test " ++ n) >> io

defaultMainQSem = sequence_ tests
------

tests = [
    -- testCase "sem1" sem1,
    -- testCase "sem2" sem2,
    -- testCase "sem_kill" sem_kill,
    testCase "sem_fifo" sem_fifo
    -- testCase "sem_bracket" sem_bracket
 ]


sem_fifo :: Assertion
sem_fifo = do
  (i,o) <- newChan
  q <- new 0
  t1 <- forkIO $ do wait q; writeChan i 'a'
  threadDelay 10000
  t2 <- forkIO $ do wait q; writeChan i 'b'
  threadDelay 10000
  t3 <- forkIO $ do wait q; writeChan i 'c'
  threadDelay 10000
  signal q
  a <- readChan o
  signal q
  b <- readChan o
  signal q
  c <- readChan o
  [a,b,c] @?= "abc"

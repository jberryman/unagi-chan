module Qsem001 where

import Control.Concurrent.QSem as OldQ

import qualified Control.Concurrent.Chan.Unagi as U
import Control.Concurrent (forkIO, threadDelay)
import Control.Monad


-- --------------------------
-- NOTE: these are tests for QSem, but we'd like to be sure our Chan doesn't
-- break them. Probably remove this.
-- --------------------------

--------
-- dummy test-framework

type Assertion = IO ()

(@?=) :: (Show a, Monad m, Eq a) => a -> a -> m ()
x @?= y = when (x /= y) $ error (show x ++ " /= " ++ show y)

testCase :: String -> IO () -> IO ()
testCase n io = putStrLn ("test " ++ n) >> io

defaultMainQSem :: IO ()
defaultMainQSem = sequence_ tests
------

tests :: [IO ()]
tests = [
    -- testCase "sem1" sem1,
    -- testCase "sem2" sem2,
    -- testCase "sem_kill" sem_kill,
    testCase "sem_fifo" sem_fifo
    -- testCase "sem_bracket" sem_bracket
 ]


sem_fifo :: Assertion
sem_fifo = do
  (i,o) <- U.newChan
  q <- newQSem 0
  _ <- forkIO $ do waitQSem q; U.writeChan i 'a'
  threadDelay 10000
  _ <- forkIO $ do waitQSem q; U.writeChan i 'b'
  threadDelay 10000
  _ <- forkIO $ do waitQSem q; U.writeChan i 'c'
  threadDelay 10000
  signalQSem q
  a <- U.readChan o
  signalQSem q
  b <- U.readChan o
  signalQSem q
  c <- U.readChan o
  [a,b,c] @?= "abc"

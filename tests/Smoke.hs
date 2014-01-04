module Smoke where

import Control.Monad
import System.Environment
import Control.Concurrent.MVar
import Control.Concurrent
import qualified Control.Concurrent.Chan.Split as S
import qualified Control.Concurrent.Chan as C

import Data.List

testContention :: Int -> Int -> Int -> IO ()
testContention writers readers n = do
  let nNice = n - rem n (lcm writers readers)
      groups = map (\i-> [i.. i - 1 + nNice `quot` writers]) $ [1, (nNice `quot` writers + 1).. nNice]
                     -- force list; don't change --
  putStrLn $ "Sending "++(show $ length $ concat groups)++" messages, with "++(show readers)++" readers and "++(show writers)++" writers."

  out <- C.newChan

  (i,o) <- S.newSplitChan
  -- some will get blocked indefinitely:
  replicateM readers $ forkIO $ forever $
      S.readChan o >>= C.writeChan out
  mapM_ (forkIO . mapM_ (S.writeChan i)) groups

  ns <- replicateM nNice (C.readChan out)
  isEmpty <- C.isEmptyChan out
  if sort ns == [1..nNice] && isEmpty
      then putStrLn $ "Success, with disorder "++(show (1 / kendTau ns))++" (closer to 1 means we have higher confidence in the test)."
      else error "What we put in isn't what we got out :("

-- kendall tau distance against the ordered list. Stupid measure of what we
-- mean by disorder
kendTau :: Ord a=> [a] -> Float
kendTau l = (pairsTot - fromIntegral (kt l)) / pairsTot where
    kt [] = 0
    kt (a:as) = (length $ filter (<a) as) + kt as
    size = fromIntegral $ length l
    pairsTot = (size * (size - 1) / 2)


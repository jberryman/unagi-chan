{-# LANGUAGE BangPatterns #-}
module Smoke where

import Control.Monad
import System.Environment
import Control.Concurrent.MVar
import Control.Concurrent
import qualified Control.Concurrent.Chan.Unagi as S
-- import qualified Control.Concurrent.Chan.Split as S
import qualified Control.Concurrent.Chan as C

import Data.List

testContention :: Int -> Int -> Int -> IO ()
testContention writers readers n = do
  let nNice = n - rem n (lcm writers readers)
             -- e.g. [[1,2,3,4,5],[6,7,8,9,10]] for 2 2 10
      groups = map (\i-> [i.. i - 1 + nNice `quot` writers]) $ [1, (nNice `quot` writers + 1).. nNice]
                     -- force list; don't change --
  out <- C.newChan

  (i,o) <- S.newChan
  -- some will get blocked indefinitely:
  replicateM readers $ forkIO $ forever $
      S.readChan o >>= C.writeChan out
  
  putStrLn $ "Sending "++(show $ length $ concat groups)++" messages, with "++(show readers)++" readers and "++(show writers)++" writers."
  mapM_ (forkIO . mapM_ (S.writeChan i)) groups

  ns <- replicateM nNice (C.readChan out)
  isEmpty <- C.isEmptyChan out
  if sort ns == [1..nNice] && isEmpty
      -- then putStrLn $ "Success!"
      -- TODO this is too slow for now:
      then let disorder = kendTau $ take 10000 ns
            in if disorder < 0.2
                 then putStrLn $ "Not enough disorder in samples: "++(show $ disorder)++". Please try again or report a bug"
                 else putStrLn $ "Success, with disorder sample of "++(show $ disorder)++" (closer to 1 means we have higher confidence in the test)."
      else error "What we put in isn't what we got out :("

-- TODO more efficient algorithm
--      more appropriate measure of disorder or different payloads
-- normalized kendall tau distance against the ordered list. Stupid measure of
-- what we mean by disorder, given how we've defined `groups` above:
kendTau :: Ord a=> [a] -> Float
kendTau ns = fromIntegral (kt ns) / pairsTot where
    kt [] = 0
    kt (a:as) = (length $ filter (<a) as) + kt as
    size = fromIntegral $ length ns
    pairsTot = (size * (size - 1) / 2)

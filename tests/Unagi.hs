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

{- THIS PASSES, BUT IN FACT GIVES US SEGFAULTS IN OUR BENCHMARKS
  Relevant Segfault-y variation from `newSegmentSource`
  -------
    return $ do
        arrClone <- P.cloneMutableArray arr 0 sEGMENT_LENGTH
        _ <- P.unsafeFreezeArray arrClone  -- NOTE [1]
        return arrClone
  -- [1] A bit of a hack to keep this array out of the remembered set, avoiding
  -- bad GC behavior as in (1). Note that each cell is only written to once
  -- after initialization, and that initial element is always shared. For this
  -- use-case everything is working against us to do extra work. See also (1,2)
  --  1) http://stackoverflow.com/questions/23462004/code-becomes-slower-as-more-boxed-arrays-are-allocated/23557704
  --  2) https://ghc.haskell.org/trac/ghc/wiki/Commentary/Rts/Storage/GC/RememberedSets#Mutableobjects:MUT_VARMVAR
  --  3) https://ghc.haskell.org/trac/ghc/wiki/Commentary/Rts/Storage/GC/EagerPromotion
  -------

import System.Mem(performGC)

-- Test our array freezing trick used in `newSegmentSource`. We rely on other
-- tests to catch other problems.
arrayFreezing = do
  sig <- newEmptyMVar
  performGC -- -- DO THIS A LOT
  var <- newEmptyMVar
  performGC -- -- DO THIS A LOT
  -- making sure var is GC'd at some point:
  mkWeakMVar var (putMVar sig True)
  performGC -- -- DO THIS A LOT

  -- this is how we create our segments:
  toCopy <- P.newArray 2 (Just var)
  performGC -- -- DO THIS A LOT
  arr1 <- P.cloneMutableArray toCopy 0 2
  performGC -- -- DO THIS A LOT
  P.unsafeFreezeArray arr1
  performGC -- -- DO THIS A LOT

  P.writeArray arr1 0 Nothing
  performGC -- -- DO THIS A LOT
  -- make sure chance for var to be collected, but isn't
  Just var' <- P.readArray arr1 1
  performGC -- -- DO THIS A LOT
  putMVar var' ()
  performGC -- -- DO THIS A LOT
  P.writeArray arr1 1 Nothing
  performGC -- -- DO THIS A LOT

  -- preserve MVar for finalizer, also acts as timeout
  x <- forkIO (threadDelay 10000000 >> putMVar sig False)
  performGC -- -- DO THIS A LOT
  wasGCd <- takeMVar sig
  performGC -- -- DO THIS A LOT
  throwTo x ThreadKilled
  performGC -- -- DO THIS A LOT
  unless wasGCd $
       error "It seems the initial mutable array element wasn't GC'd after references disappeared"
       -}

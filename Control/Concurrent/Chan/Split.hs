module Control.Concurrent.Chan.Split (
    -- * Creating channels
      newSplitChan
    , InChan(), OutChan()
    -- * Channel operations
    -- ** Reading
    , readChan
    , getChanContents
    -- ** Writing
    , writeChan
    , writeList2Chan
    ) where

-- For 'writeList2Chan', as in vanilla Chan
import System.IO.Unsafe ( unsafeInterleaveIO ) 
import Control.Concurrent.MVar
import Control.Exception (mask_, onException, evaluate)
import Data.Typeable

import Control.Concurrent(forkIO)
import Control.Concurrent.Chan.Split.Internal
import Data.IORef 







newSplitChan :: IO (InChan a, OutChan a)
{-# INLINABLE newSplitChan #-}
newSplitChan = do
   hole  <- newEmptyMVar
   readVar  <- newMVar hole
   writeVar <- newIORef hole
   return (InChan writeVar, OutChan readVar)

writeChan :: InChan a -> a -> IO ()
{-# INLINABLE writeChan #-}
writeChan (InChan writeVar) a = do
  new_hole <- newEmptyMVar
  mask_ $ do
      -- other writers can go
      old_hole <- atomicModifyIORef' writeVar ((,) new_hole)
      -- first reader can go
      putMVar old_hole (Cons a new_hole)


readChan :: OutChan a -> IO a
{-# INLINABLE readChan #-}
readChan (OutChan readVar) = do
  modifyMVarMasked readVar $ \read_end -> do -- Note [modifyMVarMasked]
    (Cons a new_read_end) <- takeMVar read_end
    return (new_read_end, a)


-- | Return a lazy list representing the contents of the supplied OutChan, much
-- like System.IO.hGetContents.
getChanContents :: OutChan a -> IO [a]
getChanContents ch = unsafeInterleaveIO (do
                            x  <- readChan ch
                            xs <- getChanContents ch
                            return (x:xs)
                        )

-- | Write an entire list of items to a chan type. Writes here from multiple
-- threads may be interleaved, and infinite lists are supported.
writeList2Chan :: InChan a -> [a] -> IO ()
{-# INLINABLE writeList2Chan #-}
writeList2Chan ch = sequence_ . map (writeChan ch)

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


-- research data dependencies and memory barriers
--    in other langs we need a "data dependency barrier"; what about in haskell?
--
-- think carefully about re-ordering potential
--   data dependencies help?
--   think about multiple *operations* inlined entailing some re-ordering
--
-- ultimately choose the partial chan that has the best performance with 1w1r
--
-- put together an implementation with striping.
-- prove correct.
--   especially pay attention to what happens when a reader blocks (because a thread descheduled) and subsequent reader completes


-- Performance TODO:
-- compare with Chan; why the bigger hit from write-all-read-all?
-- do some profiling and look at core for allocations
-- consider an optimization that on read compares head and tail MVars for
--   equality and busy waits for a time before possibly blocking

-- NOTES:
--   readArray         = 8.7ns   (OK. this pair is ~ 2ns slower than IORef)
--   writeMutableArray = 4.58ns
--   mutableArrayCASMod = 15.93ns (GOOD! Faster than atomicModifyIORef)
--
--   readIORef = 3.74ns
--   writeIORef = 7.33ns
--   atomicModifyIORef' = 23.05ns
--
--   TODO: compare arrays in 7.8

newSplitChan :: IO (InChan a, OutChan a)
{-# INLINABLE newSplitChan #-}
newSplitChan = do
   hole  <- newEmptyMVar
   readVar  <- newIORef hole
   writeVar <- newIORef hole
   return (InChan writeVar, OutChan readVar)

writeChan :: InChan a -> a -> IO ()
{-# INLINABLE writeChan #-}
writeChan (InChan writeVar) = \a-> do
  new_hole <- newEmptyMVar
  mask_ $ do
      -- NOTE: as long as this is some atomic operation, and the rest of our
      --       code remains free of fragile lockfree logic, I think we should
      --       be immune to re-ordering concerns.
      -- other writers can go
      old_hole <- atomicModifyIORef' writeVar ((,) new_hole)
      -- first reader can go
      putMVar old_hole (Cons a new_hole)


-- INVARIANT: readChan never breaks spine of queue
--     this should be very immune to re-ordering; anything that we put into
--     readVar is still correct

readChan :: OutChan a -> IO a
{-# INLINABLE readChan #-}
readChan (OutChan readVar) = readIORef readVar >>= mask_ . follow 
  where follow read_end = do
          cns <- takeMVar read_end
          case cns of
            ConsEmpty new_read_end -> do
                putMVar read_end cns
                follow new_read_end
            Cons a new_read_end -> do 
                -- NOTE: because this never breaks the spine we can use
                -- writeIORef here even if another reader races ahead of us at
                -- this point; in which case new_read_end will hold a ConsEmpty
                putMVar read_end (ConsEmpty new_read_end)    -- TODO
                writeIORef readVar new_read_end              -- TODO which order to place these? 
                return a
{-
  modifyMVarMasked readVar $ \read_end -> do -- Note [modifyMVarMasked]
    (Cons a new_read_end) <- takeMVar read_end
    return (new_read_end, a)
-}

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

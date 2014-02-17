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


-- check which IO array would be fastest for storing IORefs, in place of readVar/writeVar
--    we already get that with atomic-primops, with MutableArray, supporting CAS
--    ...although a simple `write` is probably sufficient if it doesn't affect
--      correctness and we don't expect contention on each individual Chan to be
--      likely
--    benchmark write and read of MutableArray
--
-- research data dependencies and memory barriers
--    in other langs we need a "data dependency barrier"; what about in haskell?
-- benchmark michael-scott queue on 2-cores
--
-- design blocking chan that uses IORef on write end
--    read_end <- readForCAS readVar
--    loop $ modifyMaybeFollow read_end
--    -- here or in modifyMaybeFollow CAS readVar
--    where modifyMaybeFollowOp (Cons a next) = (return a, DeadCons next)
--                              (DeadCons next) = (follow next, DeadCons next) ... or something....
--
--    ...or use IORefs but have a 
--    data List a = ... | Nil Bool MVar -- where Bool says "blocking" or not
--    when filled it is simply `readMVar`-ed to get to next element.
--        ...then we just need to figure out how to keep using it for blocking after it gets filled...
--
-- think carefully about re-ordering potential
--   data dependencies help?
--   think about multiple *operations* inlined entailing some re-ordering
--
-- see if you can replace any with atomicModifyIORefCAS
--
-- ultimately choose the partial chan that has the best performance with 1w1r
--
-- put together an implementation with striping.
-- prove correct.
--   especially pay attention to what happens when a reader blocks (because a thread descheduled) and subsequent reader completes



newSplitChan :: IO (InChan a, OutChan a)
{-# INLINABLE newSplitChan #-}
newSplitChan = do
   hole  <- newEmptyMVar
   readVar  <- newIORef hole
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


-- INVARIANT: readChan never breaks spine of queue

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

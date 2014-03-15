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
--   compare with Chan; why the bigger hit from write-all-read-all?
--   do some profiling and look at core for allocations

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

-- PROFILING
--   - compare heap size in test of TQUeue version; see if a double array of [a] might be just the ticket
--      - measure heap size of linked list
--   - look at heap size of MVar of Cons, vs an array
--   - benchmark array creation
--   - ...MVector grow
--   - benchmark chase/lev queue (see if our partial queues should really be somehow allocating into arrays)
--      - only do single-threaded and single producer variants
--   - if promising (and should be) then read chase-lev paper for idea on how array growing/ reuse would work and implement.
--
--   - TODO the simple read-some-write-some example would be better for looking
--      at core, and profiling, AND heap profiling!
--   - space profile WITH inlines in
--      - mostly Cons's and MVars
--          - maybe IORef is smaller heap size? (20 vs 36.. maybe more in a real optimized program)
--          - maybe there is an unboxed MVar somewhere?
--
--          - NOPE... but perhaps we could do a 2-element array instead of Cons+ MVar
--              - TODO benchmark 2-element MutableArray allocation.
--          - ...or just replace linked list with growable arrays??? 
--
--   - time profile and look at core with NOINLINE
--
--   - what about the space of MVar vs IORef?
--     - maybe use ReplaceAbleLock a = IORef (MVar a) -- with IORef spine version
--     - we can also replace IORefs with ForeignPtr, to remove a level of indirection (apparently, see: https://ghc.haskell.org/trac/ghc/ticket/8158#no1)
--   - can we get an unboxed MVar (maybe roll by hand?)

-- SIZE AND TIME INFO  (with size of list element subtracted)
--  Overhead per element:
--   MVar Cons Maybe         = 48 bits (...8 for Just ?, 32 for empty MVar, 8 for Cons itself ?)
--   IORef List of Maybe     = 32 bits (should be about 16 less overhead than MVar?)
--   MutableArray of Maybe    =~ 5/cell , + 8/Just ?
--                            = 13 bits? 
--                            -- NOTE: in place of MutableArray of silos for above and below, which would add a constant factor
--   Maybe [?]     = 10 bits
--      ...which we can re-use if sufficiently clever (fewer allocations)
--
--
--   Time per cell creation:
--     IORef        7ns
--     newEmptyMVar 7ns (+9ns for put)
--     MVector      3-4ns
--
--   Time for read/write or 
--     MutableArray (MVector) READ    8ns
--                            WRITE   5ns
--     IORef atomic mod              21ns
--     modifyMVar                  14-27ns ??
--
-- Total latency time count:
--     (note: in real measurements, in 7.8, chan is 62ns while TQueue is 164ns
--     TQueue style: (20ns cons, 20 ns/elem dequeue,  (+23ns/elem outer atomicmodify *2))
--
--     TQueue using a vector: 5ns write + 8ns read + counter*2
--
--
-- FOREIGN POINTER EXAMPLE:
--
--data IT a = IT { 
-- 		      tabArr  :: {-# UNPACK #-} !(Arr (Bucket a)) 
-- 		    , tabSize :: {-# UNPACK #-} !(ForeignPtr Int) 
-- 		    } 
-- 	new_ :: Int -> IO (IT a) 
--	new_ capacity = do 
--	  arr <- Arr.new Empty capacity 
--	  size <- mallocForeignPtr 
--	  withForeignPtr size $ \ptr -> poke ptr 0 
--	  return IT { tabArr = arr 
--	            , tabSize = size 
--	            } 
--
-- OTHER ENHANCEMENTS (for single reader, like actors):
--   - prefetching ?
--     - only works on MutableByteArrays and Addr
--   - shortcuts when single reader ?
--   - if busy-waits are acceptable, we could make things a fair bit speedier
--   AND VARIATIONS:
--       - 1 reader many writers, or vice versa
--       - busy-waiting acceptable (uses 'yield' instead) 
--           - might be more promising for mutable unboxed, as well as boxed
--             - Boxed: write +  read (14ns), Unboxed: write/write + read/read (15.5ns... so MutableByteArray still not a win!)
--       - NOTE: only what we can provide a solid API for... probably.
--
-- TODO
--   - This module will become Tako
--   - we will modify this to use readMVar along with counter-tagged values

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
  newHole <- newEmptyMVar
  mask_ $ do
      -- NOTE: as long as this is some atomic operation, and the rest of our
      --       code remains free of fragile lockfree logic, I think we should
      --       be immune to re-ordering concerns.
      -- other writers can go
      oldHole <- atomicModifyIORef' writeVar ((,) newHole)
      -- first reader can go
      putMVar oldHole (Cons (Just a) newHole)


-- INVARIANT: readChan never breaks spine of queue
--     this should be very immune to re-ordering; anything that we put into
--     readVar is still correct

readChan :: OutChan a -> IO a
{-# INLINABLE readChan #-}
readChan (OutChan readVar) = readIORef readVar >>= mask_ . follow 
  where follow end = do
          cns@(Cons ma newEnd) <- takeMVar end
          case ma of
            Nothing -> do
                putMVar end cns
                follow newEnd
            Just a -> do 
                -- NOTE: because this never breaks the spine we can use
                -- writeIORef here even if another reader races ahead of us at
                -- this point; in which case newEnd will hold a ConsEmpty
                putMVar end $ Cons Nothing newEnd -- \
                writeIORef readVar newEnd         -- / reorderable
                return a


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

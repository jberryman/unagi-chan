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
import Control.Exception (mask_, onException)
import Data.Typeable

import Control.Concurrent(forkIO)
import Control.Concurrent.Chan.Split.Internal


-- TODO look closely at the two distinct timing samples for the 2x2 test

-- -----
-- TODO s
--   - move in performance benchmarks and start regression testing
--   - blackholing
--   - other op
--   - write/read many, readAll
-- -----

-- TODO consider making the MVar in Negative contain a stack if the next writer
-- is a group write.
--
-- TODO consider replacing write side with `writeChan` IO action
--       - this would make for easy blackholing
--       - would make the new Positive/Negative constructor unnecessary
--       - actually, this could be very clever; the writer function need only put a list
--            ...but would we need another MVar...?
--
-- TODO test having the newly-unblocked reader check again for new messages on
-- write side before unblocking readers, maybe even using `yield`
--
-- TODO consider creating a newMVar just once always to be passed to the writer
-- on Negative (only ever written to be one)
--
-- OPTIMIZATIONS TODO
--   - create a very simple 'Main' for looking at core
--   - optimizing branches (try this last)
--      - Debug.Trace to see branches taken for different tests
--      - look at case alternatives order
--      - adding NOINLINE where clauses for unlikely or already slow, see #849

emptyStack :: Stack a
emptyStack = Positive []


-- | Read the next value from the output side of a chan.
readChan :: OutChan a -> IO a
{-# INLINABLE readChan #-}
readChan (OutChan w r) = mask_ $ do
    dequeued <- takeMVar r  -- INTERRUPTIBLE; okay
    case dequeued of
         (a:as) -> do putMVar r as 
                      return a
         [] -> do pzs <- takeMVar w -- INTERRUPTIBLE; replace `dequeued`
                            `onException` (putMVar r [])
                  case pzs of 
                    (Positive zs) ->
                      case reverse zs of
                        (a:as) -> do
                            putMVar w emptyStack -- unblocking writers ASAP
                            putMVar r as -- unblocking other readers with tail
                            return a
                        [] -> do
                            this <- newEmptyMVar
                            putMVar w (Negative this) -- unblocking writers
                            -- (*) BLOCK until writer delivers:
                            a <- takeMVar this -- INTERRUPTIBLE; wait for next writer in forked thread
                                    `onException`
                                        forkIO (do a<-takeMVar this `onException` putMVar r []; putMVar r [a])
                                        -- Will this fire on BlockedIndefinitely ?

                            -- INVARIANT: `w` becomes `Positive` before we unblock above
                            putMVar r [] -- unblocking other readers
                            return a

                    (Negative _) -> error "a Negative write side should only be visible to writers"
                    AWhistlingVoid -> error "Write side marked dead when there were more readers"


-- | Write a value to the input side of a chan.
writeChan :: InChan a -> a -> IO ()
{-# INLINABLE writeChan #-}
writeChan (InChan w) = \a -> mask_ $ do
    st <- takeMVar w -- INTERRUPTIBLE; okay
    case st of 
         (Positive as) -> putMVar w $ Positive (a:as)
         (Negative waiter) -> do 
            -- N.B. must not reorder
            putMVar w emptyStack -- unblocking other writers
            putMVar waiter a -- unblocking first reader (*)
         -- INVARIANT: this does not change for the duration of the program
         AWhistlingVoid -> 
          -- unconcerned $
              return ()

{-
-- | Returns @True@ if the runtime is certain that the channel has no more
-- readers. This may return @False@ even when no other reads are possible, and
-- is not guaranteed to return @True@ in any timely manner.
--
-- A 'writeChan' on an 'InChan' that would return @True@ here is a no-op, so
-- it's not necessary to use this to prevent space leaks.
isDefinitelyDead :: InChan a -> Bool
{-# INLINABLE isDefinitelyDead #-}
isDefinitelyDead (InChan w) = do
    st <- readMVar w
    return $
      case st of 
           AWhistlingVoid -> True
           _ -> False
-}

-- | Create a new channel, returning read and write ends.
newSplitChan :: IO (InChan a, OutChan a)
{-# INLINABLE newSplitChan #-}
newSplitChan = do
    w <- newMVar emptyStack
    r <- newMVar []
    -- A finalizer to black-hole writes once all readers disappear.
    mkWeakMVar r $
        modifyMVar_ w (const $ return AWhistlingVoid)
        -- NOTE: This means we must be very careful to keep the read var alive
        -- if we want to do reads using the Internals, e.g. when we know there
        -- is only one reader.
        -- TODO a version in Internal that returns an IO finalizer action with chans.
    return (InChan w, OutChan w r)




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

{-
-- TODO implement, see if worth restructuring again to avoid the double
-- 'reverse'. Perhaps reverse . reverse gets rewrittern? If so make a note to
-- keep constructor lazy 
--
-- Then add rewrite rules. Make sure it works with replicateM.
--
-- | Like 'writeList2Chan' but writes the entire finite list before 
atomicallyWrite

atomicallyReadN

atomicallyReadAll
-}

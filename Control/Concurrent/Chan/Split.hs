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
import Control.Exception (mask_, evaluate)
import Data.Typeable

import Control.Concurrent.Chan.Split.Internal



-- TODO are we handling exceptions correctly?

-- | Read the next value from the output side of a chan.
readChan :: OutChan a -> IO a
readChan (OutChan w r) = mask_ $ do  -- N.B. mask_
    dequeued <- takeMVar r
    case dequeued of
         (a:as) -> do putMVar r as 
                      return a
         [] -> do pzs <- takeMVar w
                  case pzs of 
                    (Positive [])-> do
                        this <- newEmptyMVar
                        -- unblock writers:
                        putMVar w (Negative this)
                        -- block until writer delivers:
                        a <- takeMVar this  -- (*)
                     -- INVARIANT: `w` is `Positive` before this point
                        -- unblock other readers:
                        putMVar r [] 
                        return a
                    (Positive zs) -> do
                        -- unblock writers ASAP:
                        putMVar w emptyStack
                        (a:as) <- evaluate $ reverse zs
                        -- unblock other readers:
                        putMVar r as
                        return a
                    _ -> error "Invariant broken: a Negative write side should only be visible to writers"


-- | Write a value to the input side of a chan.
writeChan :: InChan a -> a -> IO ()
writeChan (InChan w) = \a -> mask_ $ do  -- N.B. mask_
    st <- takeMVar w
    case st of 
         (Positive as) -> putMVar w $ Positive (a:as)
         (Negative waiter) -> do 
            -- unblock other writers:
            putMVar w emptyStack -- N.B. must not reorder
            -- unblock first reader (*):
            putMVar waiter a
                 


-- | Create a new channel, returning read and write ends.
newSplitChan :: IO (InChan a, OutChan a)
newSplitChan = do
    w <- newMVar emptyStack
    r <- newMVar []
    return (InChan w, OutChan w r)



-- takeAll -- actually we need internal access to avoid reading readerDeq
-- takeN
-- putN


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

atomicallyTake
-}

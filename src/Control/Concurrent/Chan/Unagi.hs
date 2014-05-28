module Control.Concurrent.Chan.Unagi (
    -- * Creating channels
      newChan
    , InChan(), OutChan()
    -- * Channel operations
    -- ** Reading
    , readChan
    , readChanOnException
    , getChanContents
    -- ** Writing
    , writeChan
    , writeList2Chan
    ) where
-- TODO additonal functions:
--   - dupChan :: InChan a -> IO (OutChan a) (should be doable, with a readMVar in blocking read)
--   - write functions optimized for single-writer
--   - faster write/read-many that increments counter by N

import Control.Concurrent.Chan.Unagi.Internal
-- For 'writeList2Chan', as in vanilla Chan
import System.IO.Unsafe ( unsafeInterleaveIO ) 


newChan :: IO (InChan a, OutChan a)
newChan = newChanStarting (maxBound - 10) 
    -- lets us test counter overflow in tests and normal course of operation

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


-- TODO checklist
-- - implement additional tests and assertions for Unagi
-- - add a benchmark and validate Unagi approach (move from chan-benchmarks)
-- - add some more advanced behavior in notes, re. memory effects / wait-free-ness
-- - examine some behavior via eventlog; maybe create a script to do automated analysis of it.
-- - add atomic-primop tests
-- - figure out naming and module structure
--      - "fishy-chans", with code-named modules: Tako, Unagi
-- - consider travis CI
-- - either release or finish Tako variant, and set up tests for it.

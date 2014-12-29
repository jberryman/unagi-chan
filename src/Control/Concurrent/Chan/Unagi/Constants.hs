module Control.Concurrent.Chan.Unagi.Constants 
    where

-- Constants for boxed and unboxed unagi.

import Data.Bits
import Control.Exception(assert)

divMod_sEGMENT_LENGTH :: Int -> (Int,Int)
{-# INLINE divMod_sEGMENT_LENGTH #-}
divMod_sEGMENT_LENGTH n = let d = n `unsafeShiftR` lOG_SEGMENT_LENGTH
                              m = n .&. sEGMENT_LENGTH_MN_1
                           in d `seq` m `seq` (d,m)

-- Nexttant for now: back-of-envelope considerations:
--   - making most of constant factor for cloning array of *any* size
--   - make most of overheads of moving to the next segment, etc.
--   - provide enough runway for creating next segment when 32 simultaneous writers 
--   - the larger this the larger one-time cost for the lucky writer
--   - as arrays collect in heap, performance might suffer, so bigger arrays
--     give us a constant factor edge there. see:
--       http://stackoverflow.com/q/23462004/176841
--
sEGMENT_LENGTH :: Int
{-# INLINE sEGMENT_LENGTH #-}
sEGMENT_LENGTH = 1024 -- NOTE: THIS MUST REMAIN A POWER OF 2!

-- Number of reads on which to spin for new segment creation.
-- Back-of-envelope (time_to_create_new_segment / time_for_read_IOref) + margin.
-- See usage site.
--
-- NOTE: this was calculated for boxed Unagi, but it probably doesn't make a
-- measurable difference that we use it for Unagi.Unboxed too.
nEW_SEGMENT_WAIT :: Int
nEW_SEGMENT_WAIT = round (((14.6::Float) + 0.3*fromIntegral sEGMENT_LENGTH) / 3.7) + 10

lOG_SEGMENT_LENGTH :: Int
lOG_SEGMENT_LENGTH = 
    let x = 10  -- ...pre-computed from...
     in assert (x == (round $ logBase (2::Float) $ fromIntegral sEGMENT_LENGTH))
         x

sEGMENT_LENGTH_MN_1 :: Int
sEGMENT_LENGTH_MN_1 = sEGMENT_LENGTH - 1

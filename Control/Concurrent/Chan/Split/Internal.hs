{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Control.Concurrent.Chan.Split.Internal (
   -- | Unsafe implementation details. This interface will not be stable across
   -- versions.
   Stack(..), W, R, InChan(..), OutChan(..)
   ) where

import Data.Typeable(Typeable)
import Control.Concurrent.MVar

-- NOTE: using a composition list (e.g. putMVar (as . (b:))) was actually
-- slightly slower than a cons + reverse, regardless of size, however a
-- composition list could give us the ability to do a "priority" write to the
-- head of the queue in O(1).
data Stack a = Positive [a]       -- stack that writers push onto
             | Negative !(MVar a) -- first waiting reader blocked
             | Dead               -- all readers GCd

type W a = MVar (Stack a)
type R a = MVar [a]

-- | The \"write side\" of a channel.
newtype InChan a = InChan (W a)
    deriving (Eq, Typeable)

-- | The \"read side\" of a channel.
data OutChan a = OutChan !(W a) !(R a)
    deriving (Eq, Typeable)

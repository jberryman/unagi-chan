{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Control.Concurrent.Chan.Split.Internal (
   -- | Unsafe implementation details. This interface will not be stable across
   -- versions.
   Cons(..), InChan(..), OutChan(..)
   ) where

import Data.Typeable(Typeable)
import Control.Concurrent.MVar
import Data.IORef

-- TODO
--   replace with IORef
--   replace nested MVars with top-level lock + unboxed MVar

type Stream a = MVar (Cons a)
data Cons a = Cons a !(Stream a)
            | ConsEmpty !(Stream a)

-- | The \"write side\" of a chan pair
newtype InChan i = InChan (IORef (Stream i)) -- Invariant: Stream i always empty MVar
    deriving (Eq, Typeable)

-- | The \"read side\" of a chan pair
newtype OutChan i = OutChan (IORef (Stream i)) 
    deriving (Eq, Typeable)

module Implementations where

import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Unboxed as UU
import qualified Control.Concurrent.Chan.Unagi.Bounded as UB
import qualified Control.Concurrent.Chan.Unagi.NoBlocking as UN
import qualified Data.Primitive as P
import Control.Concurrent(yield)

type Implementation inc outc a = (IO (inc a, outc a), inc a -> a -> IO (), outc a -> IO a, inc a -> IO (outc a))

unagiImpl :: Implementation U.InChan U.OutChan a
unagiImpl =  (U.newChan, U.writeChan, U.readChan, U.dupChan)

unboxedUnagiImpl :: (P.Prim a)=> Implementation UU.InChan UU.OutChan a
unboxedUnagiImpl = (UU.newChan, UU.writeChan, UU.readChan, UU.dupChan)

unagiBoundedImpl :: Int -> Implementation UB.InChan UB.OutChan a
unagiBoundedImpl n =  (UB.newChan n, UB.writeChan, UB.readChan, UB.dupChan)

-- We use our yield "blocking" readChan here:
unagiNoBlockingImpl :: Implementation UN.InChan UN.OutChan a
unagiNoBlockingImpl =  (UN.newChan, UN.writeChan, UN.readChan yield, UN.dupChan)

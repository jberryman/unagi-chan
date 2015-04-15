module Implementations where

import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Unboxed as UU
import qualified Control.Concurrent.Chan.Unagi.Bounded as UB
import qualified Control.Concurrent.Chan.Unagi.NoBlocking as UN
import qualified Control.Concurrent.Chan.Unagi.NoBlocking.Unboxed as UNU
import Control.Concurrent(yield, threadDelay)

type Implementation inc outc a = (IO (inc a, outc a), inc a -> a -> IO (), outc a -> IO a, inc a -> IO (outc a))

unagiImpl , unagiTryReadImpl , unagiTryReadBlockingImpl :: Implementation U.InChan U.OutChan a
unagiImpl =  (U.newChan, U.writeChan, U.readChan, U.dupChan)
unagiTryReadImpl =  (U.newChan, U.writeChan, u_trying_readChan, U.dupChan)
unagiTryReadBlockingImpl =  (U.newChan, U.writeChan, u_trying_readChan_blocking, U.dupChan)

unboxedUnagiImpl , unboxedUnagiTryReadImpl, unboxedUnagiTryReadBlockingImpl :: (UU.UnagiPrim a)=> Implementation UU.InChan UU.OutChan a
unboxedUnagiImpl = (UU.newChan, UU.writeChan, UU.readChan, UU.dupChan)
unboxedUnagiTryReadImpl = (UU.newChan, UU.writeChan, uu_trying_readChan, UU.dupChan)
unboxedUnagiTryReadBlockingImpl = (UU.newChan, UU.writeChan, uu_trying_readChan_blocking, UU.dupChan)

unagiBoundedImpl , unagiBoundedTryReadImpl, unagiBoundedTryReadBlockingImpl :: Int -> Implementation UB.InChan UB.OutChan a
unagiBoundedImpl n =  (UB.newChan n, UB.writeChan, UB.readChan, UB.dupChan)
unagiBoundedTryReadImpl n =  (UB.newChan n, UB.writeChan, ub_trying_readChan, UB.dupChan)
unagiBoundedTryReadBlockingImpl n =  (UB.newChan n, UB.writeChan, ub_trying_readChan_blocking, UB.dupChan)

-- We use our yield "blocking" readChan here, and below:
unagiNoBlockingImpl :: Implementation UN.InChan UN.OutChan a
unagiNoBlockingImpl =  (UN.newChan, UN.writeChan, UN.readChan yield, UN.dupChan)

unagiNoBlockingUnboxedImpl :: (UU.UnagiPrim a)=> Implementation UNU.InChan UNU.OutChan a
unagiNoBlockingUnboxedImpl =  (UNU.newChan, UNU.writeChan, UNU.readChan yield, UNU.dupChan)

-- These have same semantics as corresponding `readChan`, so this is an easy
-- way to do smoke tests of `tryReadChan`:
uu_trying_readChan :: (UU.UnagiPrim a)=> UU.OutChan a -> IO a
uu_trying_readChan oc = do
    (e,_) <- UU.tryReadChan oc
    let go = UU.tryRead e >>= maybe (threadDelay 1000 >> go) return
    go

u_trying_readChan :: U.OutChan a -> IO a
u_trying_readChan oc = do
    (e,_) <- U.tryReadChan oc
    let go = U.tryRead e >>= maybe (threadDelay 1000 >> go) return
    go

ub_trying_readChan :: UB.OutChan a -> IO a
ub_trying_readChan oc = do
    (e,_) <- UB.tryReadChan oc
    let go = UB.tryRead e >>= maybe (threadDelay 1000 >> go) return
    go

-- And we want to test the blocking action of tryReadChan as well:
uu_trying_readChan_blocking :: (UU.UnagiPrim a)=> UU.OutChan a -> IO a
uu_trying_readChan_blocking oc = UU.tryReadChan oc >>= snd

u_trying_readChan_blocking :: U.OutChan a -> IO a
u_trying_readChan_blocking oc = U.tryReadChan oc >>= snd

ub_trying_readChan_blocking :: UB.OutChan a -> IO a
ub_trying_readChan_blocking oc = UB.tryReadChan oc >>= snd


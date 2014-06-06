module Implementations where

import qualified Control.Concurrent.Chan.Unagi as U
import qualified Control.Concurrent.Chan.Unagi.Unboxed as UU

type Implementation inc outc a = (IO (inc Int, outc Int), inc Int -> Int -> IO (), outc Int -> IO Int)

unagiImpl :: Implementation U.InChan U.OutChan a
unagiImpl =  (U.newChan, U.writeChan, U.readChan)

unboxedUnagiImpl :: Implementation UU.InChan UU.OutChan a
unboxedUnagiImpl = (UU.newChan, UU.writeChan, UU.readChan)


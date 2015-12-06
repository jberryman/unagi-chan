module Main
    where

import System.IO
import Control.Concurrent
import Control.Exception

-- implementation-agnostic tests:
import Deadlocks
import Smoke
import DupChan

-- implementation-specific tests:
import Unagi
import UnagiUnboxed
import UnagiBounded
import UnagiNoBlocking
import UnagiNoBlockingUnboxed

-- Other
import Atomics
import IndexedMVar
import Control.Concurrent.Chan.Unagi.Internal(assertionCanary)

main :: IO ()
main = do 
    -- Make sure testing environment is sane:
    assertionsWorking <- try $ assert False $ return ()
    assertionsWorkingInLib <- assertionCanary
    case assertionsWorking of
         Left (AssertionFailed _)
           | assertionsWorkingInLib -> putStrLn "Assertions: On"
         _  -> error "Assertions aren't working"

    procs <- getNumCapabilities
    if procs < 2 
        then error "Tests are only effective if more than 1 core is available"
        else return ()
    hSetBuffering stdout NoBuffering

    -- -----------------------------------
    -- test important properties of our atomic-primops:
    atomicsMain

    indexedMVarMain

    -- do things catch fire?
    smokeMain

    -- dupChan tests
    dupChanMain

    -- check for deadlocks:
    deadlocksMain

    -- implementation-specific tests
    unagiMain
    unagiUnboxedMain
    unagiBoundedMain
    unagiNoBlockingMain
    unagiNoBlockingUnboxedMain

    putStrLn "ALL TESTS PASSED!"

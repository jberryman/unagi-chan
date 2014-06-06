module Main
    where

import System.IO
import Control.Concurrent
import Control.Exception

-- implementation-agnostic tests:
import Qsem001
import Deadlocks
import Smoke
import DupChan

-- implementation-specific tests:
import Atomics
import Unagi

main :: IO ()
main = do 
    assertionsWorking <- try $ assert False $ return ()
    case assertionsWorking of
         Left (AssertionFailed _) -> putStrLn "Assertions: On"
         _                        -> error "Assertions aren't working"

    procs <- getNumCapabilities
    if procs < 2 
        then error "Tests are only effective if more than 1 core is available"
        else return ()
    hSetBuffering stdout NoBuffering

    -- -----------------------------------

    -- test important properties of our atomic-primops:
    atomicsMain

    -- do things catch fire?
    smokeMain

    -- dupChan tests
    dupChanMain

    -- QSem tests:
    defaultMainQSem

    -- check for deadlocks:
    deadlocksMain

    -- unagi-specific tests
    unagiMain

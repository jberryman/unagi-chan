module Main
    where

import System.IO
import System.Environment
import Control.Concurrent

import Qsem001
import Chan002
import Chan003
import Smoke

main = do 
    procs <- getNumCapabilities
    if procs < 2 
        then error "Tests are only effective if more than 1 core is available"
        else return ()
    hSetBuffering stdout NoBuffering
    testContention 2 2 1000000
    -- QSem tests:
    defaultMainQSem
    -- check for deadlocks:
    let tries = 10000
    putStrLn $ "Checking for deadlocks from killed reader, x"++show tries
    checkDeadlocksReader tries
    putStrLn $ "Checking for deadlocks from killed writer, x"++show tries
    checkDeadlocksWriter tries

-- TODO deadlock tests:
--        - use async + thread delay to determine when to start throwing kill, and to save time
--        - make sure they are effective on a 2-core machine, otherwise emit Warning!
--        - maybe combine the tests, removing Chan003
--        - in Chan002, also do a final write and then a read to make sure not broken.

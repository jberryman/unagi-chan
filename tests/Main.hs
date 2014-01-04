module Main
    where

import System.IO
import System.Environment

import Qsem001
import Chan002
import Chan003

main = do 
    hSetBuffering stdout NoBuffering
    -- QSem tests:
    defaultMainQSem
    -- "check for deadlocks":
    let tries = 100
    putStrLn $ "Checking for deadlocks x"++show tries
    mainChan002 tries
    putStrLn ""
    mainChan003 tries

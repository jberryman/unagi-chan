module TestUtils where

import System.Environment

-- For when we're being paranoid or just need to be able to skip a check
-- because CI is sucking.
error_paranoid :: String -> IO ()
error_paranoid e = do
  v <- lookupEnv "UNAGI_TESTING_LAX"
  case v of
    Nothing -> error e
    Just _true -> 
      putStrLn $ "WARNING!!: "++e

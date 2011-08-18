import KeyDatabaseSQLite

hello :: Int -> String -> Dynamic
hello = persistentSQLite "hello.db" "hello" ["hello"]

main :: IO ()
main = do
    let info = "Hello, echo!"
    Just res <- runJustT (newDBEntry hello info |>>=
                              getDB . getDBInfo hello)
    putStrLn res
    closeDBHandles

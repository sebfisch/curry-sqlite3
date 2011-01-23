import IO
import KeyDatabaseSQLite
import List ( sortBy ); sort = sortBy (<=)

testPred :: Int -> (String,Int) -> Dynamic
testPred = persistentSQLite "test.db" "test" ["string","int"]

test'notExists :: Test
test'notExists = existsDBKey testPred 0 `qYields` False

test'allKeysEmpty :: Test
test'allKeysEmpty = allDBKeys testPred `qYields` []

test'allInfosEmpty :: Test
test'allInfosEmpty = allDBInfos testPred `qYields` []

test'allKeyInfosEmpty :: Test
test'allKeyInfosEmpty = allDBKeyInfos testPred `qYields` []

test'infoEmpty :: Test
test'infoEmpty =
  getDBInfo testPred 0
    `qExitsWith` "ERROR: KeyNotExistsError: getDBInfo, 0"

test'infosEmpty :: Test
test'infosEmpty =
  getDBInfos testPred [0,1,2]
    `qExitsWith` "ERROR: KeyNotExistsError: getDBInfos, 0"

test'deleteKeyEmpty :: Test
test'deleteKeyEmpty = deleteDBEntry testPred 0 `tYields` ()

test'deleteKeysEmpty :: Test
test'deleteKeysEmpty = deleteDBEntries testPred [0,1,2] `tYields` ()

test'updateEmpty :: Test
test'updateEmpty =
  updateDBEntry testPred 0 ("",1) `tExitsWith` KeyNotExistsError

test'createdExists :: Test
test'createdExists =
  (newDBEntry testPred ("new",42) |>>= getDB . existsDBKey testPred)
    `tYields` True

test'createdGoneAfterClean :: Test
test'createdGoneAfterClean =
  (newDBEntry testPred ("new",42) |>>= \key ->
   cleanDB testPred |>> getDB (getDBInfo testPred key))
    `tExitsWith` KeyNotExistsError

test'getAllCreatedKeys :: Test
test'getAllCreatedKeys =
  (mapT (newDBEntry testPred) [("a",10),("b",20),("c",30)] |>>= \keys1 ->
   getDB (allDBKeys testPred) |>>= \keys2 ->
   returnT (sameBag keys1 keys2))
    `tYields` True

sameBag :: [a] -> [a] -> Bool
sameBag xs ys = sort xs == sort ys

test'getAllCreatedInfos :: Test
test'getAllCreatedInfos =
  let infos1 = [("a",10),("b",20),("c",30)]
   in (mapT (newDBEntry testPred) infos1 |>>
       getDB (allDBInfos testPred) |>>= \infos2 ->
       returnT (sort infos2))
        `tYields` infos1

test'getAllCreatedKeyInfos :: Test
test'getAllCreatedKeyInfos =
  let infos = [("a",10),("b",20),("c",30)]
   in (mapT (newDBEntry testPred) infos |>>= \keys ->
       let keyinfos1 = zip keys infos
        in getDB (allDBKeyInfos testPred) |>>= \keyinfos2 ->
           returnT (sameBag keyinfos1 keyinfos2))
             `tYields` True

test'getCreatedInfo :: Test
test'getCreatedInfo =
  (newDBEntry testPred ("new",42) |>>= getDB . getDBInfo testPred)
    `tYields` ("new",42)

test'getCreatedInfos :: Test
test'getCreatedInfos =
  let infos = [("a",10),("b",20),("c",30)]
   in (mapT (newDBEntry testPred) infos |>>= \keys ->
       getDB (getDBInfos testPred keys))
        `tYields` infos

test'deleteOneCreated :: Test
test'deleteOneCreated =
  (mapT (newDBEntry testPred) [("a",10),("b",20),("c",30)] |>>= \keys ->
   deleteDBEntry testPred (keys!!1) |>>
   getDB (allDBInfos testPred) |>>= \infos ->
   returnT (sort infos))
     `tYields` [("a",10),("c",30)]

test'deleteAllCreated :: Test
test'deleteAllCreated =
  (mapT (newDBEntry testPred) [("a",10),("b",20),("c",30)] |>>= \keys ->
   deleteDBEntries testPred keys |>>
   getDB (allDBKeys testPred))
     `tYields` []

test'updateCreated :: Test
test'updateCreated =
  (newDBEntry testPred ("old",41) |>>= \key ->
   updateDBEntry testPred key ("new",42) |>>
   getDB (getDBInfo testPred key))
     `tYields` ("new",42)

test'queryDeleted :: Test
test'queryDeleted =
  (newDBEntry testPred ("new",42) |>>= \key ->
   deleteDBEntry testPred key |>>
   getDB (getDBInfo testPred key))
     `tExitsWith` KeyNotExistsError

test'queryListWithOneDeleted :: Test
test'queryListWithOneDeleted =
  (mapT (newDBEntry testPred) [("a",10),("b",20),("c",30)] |>>= \keys ->
   deleteDBEntry testPred (keys!!1) |>>
   getDB (getDBInfos testPred keys))
     `tExitsWith` KeyNotExistsError

test'rollbackOnError :: Test
test'rollbackOnError = error "message" `tExitsWith` ExecutionError

-- test combinators

type Test = IO (Maybe String)

qYields :: Query a -> a -> Test
query `qYields` val =
  (runQ query >>= return . checkRes val) `catch` \ (IOError err) ->
    return . Just $ unlines
      ["exits with run-time error", '\t':show err,
       "but should yield", '\t':show val]

checkRes :: a -> a -> Maybe String
checkRes val res | res == val = Nothing
                 | otherwise  = Just $ unlines
                     ["yields", '\t':show res,
                      "but should yield", '\t':show val]

tYields :: Transaction a -> a -> Test
trans `tYields` val =
  do tres <- runT trans
     return $ case tres of
       Left res  -> checkRes val res 
       Right err -> Just $ unlines
         ["exits with transaction error", '\t':show err,
          "but should yield", '\t':show val]

qExitsWith :: Query a -> String -> Test
query `qExitsWith` msg =
  (do res <- runQ query
      return . Just $ unlines
        ["yields", '\t':show res,
         "but should exit with run-time error", '\t':show msg])
    `catch` checkError
 where
  checkError (IOError err)
    | err == msg = return Nothing
    | otherwise  = return . Just $ unlines
        ["exits with run-time error", '\t':show err,
         "but should exit with run-time error", '\t':show msg]

tExitsWith :: Transaction a -> TErrorKind -> Test
trans `tExitsWith` kind =
  (do tres <- runT trans
      return $!! case tres of
        Left res -> Just $ unlines
          ["yields", '\t':show res,
           "but should exit with transaction error", '\t':show kind]
        Right err@(TError knd _) ->
          if knd == kind then Nothing else Just $ unlines
            ["exits with transaction error", '\t':showTError err,
             "but should exit with error kind", '\t':show kind])
    `catch` \ (IOError err) -> return . Just $ unlines
      ["exits with run-time error", '\t':show err,
       "but should exit with transaction error of kind", '\t':show kind]

runTest :: String -> Test -> IO ()
runTest label t =
  do hPutStr stderr $ label ++ ", "
     test t

test :: Test -> IO ()
test execTest = 
  do runT $ cleanDB testPred
     execTest >>= maybe (hPutStrLn stderr "passes") (hPutStrLn stderr)

main :: IO ()
main =
  do runTest "notExists" test'notExists
     runTest "allKeysEmpty" test'allKeysEmpty
     runTest "allInfosEmpty" test'allInfosEmpty
     runTest "allKeyInfosEmpty" test'allKeyInfosEmpty
     runTest "infoEmpty" test'infoEmpty
     runTest "infosEmpty" test'infosEmpty
     runTest "deleteKeyEmpty" test'deleteKeyEmpty
     runTest "deleteKeysEmpty" test'deleteKeysEmpty
     runTest "updateEmpty" test'updateEmpty
     runTest "createdExists" test'createdExists
     runTest "createdGoneAfterClean" test'createdGoneAfterClean
     runTest "getAllCreatedKeys" test'getAllCreatedKeys
     runTest "getAllCreatedInfos" test'getAllCreatedInfos
     runTest "getAllCreatedKeyInfos" test'getAllCreatedKeyInfos
     runTest "getCreatedInfo" test'getCreatedInfo
     runTest "getCreatedInfos" test'getCreatedInfos
     runTest "deleteOneCreated" test'deleteOneCreated
     runTest "deleteAllCreated" test'deleteAllCreated
     runTest "updateCreated" test'updateCreated
     runTest "queryDeleted" test'queryDeleted
     runTest "queryListWithOneDeleted" test'queryListWithOneDeleted
     runTest "rollbackOnError" test'rollbackOnError

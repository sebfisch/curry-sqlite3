module KeyDatabaseSQLite3 (

  -- missing: index, sortByIndex, groupByIndex

  Query, runQ,

  Transaction, TError(..), TErrorKind(..), runT, runJustT, runTNA,
  (|>>=), (|>>), sequenceT, sequenceT_, mapT, mapT_,

  Dynamic, DBFile, TableName, keyPredSQLite3,

  existsDBKey,

  -- don't use the allDB* functions unless the database is small!
  allDBKeys, allDBInfos, allDBKeyInfos,

  getDBInfo, getDBInfos,

  deleteDBEntry, updateDBEntry, newDBEntry, cleanDB,

  closeDBHandles

  ) where

import Database     ( TError(..), TErrorKind(..) )
import Global       ( Global, GlobalSpec(..), global, readGlobal, writeGlobal )
import IO           ( Handle, hPutStrLn, hGetLine, hFlush, hClose )
import IOExts       ( connectToCommand )
import List         ( intersperse, insertBy )
import ReadNumeric  ( readInt )
import ReadShowTerm ( readQTerm, showQTerm )
import Unsafe       ( unsafePerformIO ) -- to ensure that DB and table exist

path'to'sqlite3 :: String
path'to'sqlite3 = "sqlite3"

-- Query and Transaction types

data Query a = Query (IO a)

runQ :: Query a -> IO a
runQ (Query a) = a

transformQ :: (a -> b) -> Query a -> Query b
transformQ f query = Query (runQ query >>= return . f)

data Transaction a = Trans (IO (TransResult a))
data TransResult a = OK a | Error TError

unTrans :: Transaction a -> IO (TransResult a)
unTrans (Trans action) = action

runTNA :: Transaction a -> IO (Either a TError)
runTNA trans =
  do result <- unTrans trans
     return $ case result of
       OK res    -> Left res
       Error msg -> Right msg

runT :: Transaction a -> IO (Either a TError)
runT trans = -- immediate to take locks in fixed order
  do withAllDBHandles (`hPutAndFlush` "begin immediate")
     result <- runTNA trans
     withAllDBHandles (`hPutAndFlush` "commit")
     return result

runJustT :: Transaction a -> IO a
runJustT trans =
  do Left result <- runT trans
     return result

getDB :: Query a -> Transaction a
getDB = transIO . runQ

-- not exported
transIO :: IO a -> Transaction a
transIO action = Trans (action >>= return . OK)

returnT :: a -> Transaction a
returnT = transIO . return

doneT :: Transaction ()
doneT = transIO done

errorT :: TError -> Transaction a
errorT = Trans . return . Error

failT :: String -> Transaction a
failT = errorT . TError UserDefinedError

(|>>=) :: Transaction a -> (a -> Transaction b) -> Transaction b
Trans action |>>= f = Trans $
  do result <- action
     case result of
       Error err -> return $ Error err
       OK res    -> unTrans $ f res

(|>>) :: Transaction _ -> Transaction a -> Transaction a
t1 |>> t2 = t1 |>>= const t2

sequenceT :: [Transaction a] -> Transaction [a]
sequenceT = foldr seqT (returnT [])
 where
  seqT t ts = t |>>= \x -> ts |>>= \xs -> returnT (x:xs)

sequenceT_ :: [Transaction _] -> Transaction ()
sequenceT_ = foldr (|>>) doneT

mapT :: (a -> Transaction b) -> [a] -> Transaction [b]
mapT f = sequenceT . map f

mapT_ :: (a -> Transaction _) -> [a] -> Transaction ()
mapT_ f = sequenceT_ . map f

-- Interface based on keys

type DBFile = String
type TableName = String

data Dynamic = DBInfo DBFile TableName

type Key = Int
type KeyPred a = Key -> a -> Dynamic -- for interface compatibility

dbInfo :: KeyPred a -> (DBFile,TableName)
dbInfo keyPred = (db,table)
 where
  DBInfo db table = keyPred ignored ignored

ignored :: a
ignored = error "unexpected access to argument of database predicate"

dbFile :: KeyPred _ -> DBFile
dbFile = fst . dbInfo

tableName :: KeyPred _ -> TableName
tableName = snd . dbInfo

keyPredSQLite3 :: DBFile -> TableName -> KeyPred a
keyPredSQLite3 db table _ _ = unsafePerformIO $
  do ensureDBTable db table
     return $ DBInfo db table

existsDBKey :: KeyPred _ -> Key -> Query Bool
existsDBKey keyPred key =
  transformQ (0/=) . Query . selectWhereCount keyPred $ "key = " ++ show key

-- don't use
allDBKeys :: KeyPred _ -> Query [Key]
allDBKeys keyPred = transformQ (map readIntOrExit) $ select keyPred "key"

-- don't use
allDBInfos :: KeyPred a -> Query [a]
allDBInfos keyPred = transformQ (map readQTerm) $ select keyPred "info"

-- don't use
allDBKeyInfos :: KeyPred a -> Query [(Key,a)]
allDBKeyInfos keyPred =
  transformQ (map readKeyInfo) $ select keyPred "key, info"

readKeyInfo :: String -> (Key,a)
readKeyInfo row = (readIntOrExit key, readQTerm info)
 where
  key:info:_ = words row

getDBInfo :: KeyPred a -> Key -> Query a
getDBInfo keyPred = transformQ readQTerm . selectKey keyPred "info"

getDBInfos :: KeyPred a -> [Key] -> Query [a]
getDBInfos keyPred keys =
  transformQ sortByIndexInGivenList $ selectKeys keyPred "key, info" keys
 where
  sortByIndexInGivenList rows =
    let keyInfos = map readKeyInfo rows
        addKeyInfo key infos =
          maybe (error $ "no info for key " ++ show key)
                (\info -> info : infos)
                (lookup key keyInfos)
     in foldr addKeyInfo [] keys

deleteDBEntry :: KeyPred _ -> Key -> Transaction ()
deleteDBEntry keyPred key = transIO $
  do sqlite3 0 (dbFile keyPred) $ 
       "delete from " ++ tableName keyPred ++
       " where key = " ++ show key
     done

updateDBEntry :: KeyPred a -> Key -> a -> Transaction ()
updateDBEntry keyPred key info = transIO $
  do sqlite3 0 (dbFile keyPred) $
       "update " ++ tableName keyPred ++
       " set info = " ++ quote (showQTerm info) ++
       " where key = " ++ show key
     done

quote :: String -> String
quote s = "\"" ++ concatMap quoteChar s ++ "\""
 where
  quoteChar c = if c == '"' then ['\\','"'] else [c]

newDBEntry :: KeyPred a -> a -> Transaction Key
newDBEntry keyPred info = transIO $
  do sqlite3 0 (dbFile keyPred) $
       "insert into " ++ tableName keyPred ++ " (info) " ++
       " values (" ++ quote (showQTerm info) ++ ")"
     [n] <- sqlite3 1 (dbFile keyPred) "select last_insert_rowid()"
     return $ readIntOrExit n

cleanDB :: KeyPred _ -> Transaction ()
cleanDB keyPred = transIO $
  do sqlite3 0 (dbFile keyPred) $ "delete from " ++ tableName keyPred
     done

-- SQL query functions

type Row = String

select :: KeyPred _ -> String -> Query [Row]
select keyPred cols = Query $
  do let sql = "select " ++ cols ++ " from " ++ tableName keyPred
     n <- selectCount keyPred
     putStrLn $ unlines
       ["Warning: select statement without condition. ",
        sql,
        "This query has " ++ show n ++ " results."]
     sqlite3 n (dbFile keyPred) sql

selectCount :: KeyPred _ -> IO Int
selectCount keyPred =
  do [count] <- sqlite3 1 (dbFile keyPred) $
       "select count(*) from " ++ tableName keyPred
     return $ readIntOrExit count

-- yields 1 for "1a" and exits for ""
readIntOrExit :: String -> Int
readIntOrExit s =
  maybe (error $ "cannot read Int form String " ++ show s) fst $ readInt s

selectWhereCount :: KeyPred _ -> String -> IO Int
selectWhereCount keyPred cond =
  do [count] <- sqlite3 1 (dbFile keyPred) $
       "select count(*) from " ++ tableName keyPred ++ " where " ++ cond
     return $ readIntOrExit count

selectWhere :: KeyPred _ -> String -> String -> Query [String]
selectWhere keyPred cols cond = Query $
  do n <- selectWhereCount keyPred cond
     sqlite3 n (dbFile keyPred) $
       "select " ++ cols ++ " from " ++ tableName keyPred ++ " where " ++ cond

selectKey :: KeyPred _ -> String -> Key -> Query Row
selectKey keyPred cols key =
  transformQ firstRow . selectWhere keyPred cols $ "key = " ++ show key
 where
  firstRow []      = error $ "no info for key " ++ show key
  firstRow (row:_) = row

selectKeys :: KeyPred _ -> String -> [Key] -> Query [Row]
selectKeys keyPred cols keys = 
  selectWhere keyPred cols $
    "key in (" ++ intercalate "," (map show keys) ++ ")"

intercalate :: [a] -> [[a]] -> [a]
intercalate l = concat . intersperse l

sqlite3 :: Int -> String -> String -> IO [Row]
sqlite3 n db sql = 
  do h <- getDBHandle db
     hPutAndFlush h $ sql ++ ";"
     sequenceIO . replicate n $ hGetLine h

hPutAndFlush :: Handle -> String -> IO ()
hPutAndFlush h s = hPutStrLn h s >> hFlush h

-- Globally stored information

existingDBTables :: Global [(DBFile,TableName)]
existingDBTables = global [] Temporary

ensureDBTable :: DBFile -> TableName -> IO ()
ensureDBTable db table =
  do dbTables <- readGlobal existingDBTables
     unless ((db,table) `elem` dbTables) $ do
       sqlite3 0 db $
         "create table if not exists " ++ table ++
         " (key integer primary key asc autoincrement, info not null)"
       writeGlobal existingDBTables ((db,table):dbTables)

unless :: Bool -> IO () -> IO ()
unless True  _      = done
unless False action = action

openDBHandles :: Global [(DBFile,Handle)]
openDBHandles = global [] Temporary

withAllDBHandles :: (Handle -> IO _) -> IO ()
withAllDBHandles f =
  do dbHandles <- readGlobal openDBHandles
     mapIO_ (f . snd) dbHandles

closeDBHandles :: IO ()
closeDBHandles = withAllDBHandles hClose

getDBHandle :: DBFile -> IO Handle
getDBHandle db =
  do dbHandles <- readGlobal openDBHandles
     let createDBHandle =
           do h <- connectToCommand $ path'to'sqlite3 ++ " " ++ db
              writeGlobal openDBHandles $ -- sort against deadlock
                insertBy ((<=) `on` fst) (db,h) dbHandles
              return h
     maybe createDBHandle return $ lookup db dbHandles

on :: (b -> b -> c) -> (a -> b) -> a -> a -> c
(f `on` g) x y = f (g x) (g y)


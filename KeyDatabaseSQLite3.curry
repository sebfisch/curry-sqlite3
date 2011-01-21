------------------------------------------------------------------------------
--- This module provides a general interface for databases (persistent
--- predicates) where each entry consists of a key and an info
--- part. The key is an integer and the info is arbitrary. All
--- functions are parameterized with a dynamic predicate that takes an
--- integer key as a first parameter.
---
--- This module reimplements the interface of the module
--- <code>KeyDatabase</code> based on the
--- <a href="http://sqlite.org/">SQLite</a> database engine.
--- In order to use it you need to have <code>sqlite3</code> in your
--- <code>PATH</code> environment variable or adjust the value of the
--- constant <code>path'to'sqlite3</code>.
---
--- Programs that use the <code>KeyDatabase</code> module can be adjusted
--- to use this module instead by replacing the imports of
--- <code>Dynamic</code>, <code>Database</code>, and
--- <code>KeyDatabase</code> with this module and changing the declarations
--- of database predicates to use the function <code>persistentSQLite3</code>
--- instead of <code>dynamic</code> or <code>persistent</code>.
--- This module redefines the types <code>Dynamic</code>,
--- <code>Query</code>, and <code>Transaction</code> and although both
--- implementations can be used in the same program (by importing modules
--- qualified) they cannot be mixed.
---
--- Compared with the interface of <code>KeyDatabase</code>, this module
--- lacks definitions for <code>index</code>, <code>sortByIndex</code>,
--- <code>groupByIndex</code>, and <code>runTNA</code> and adds the
--- functions <code>deleteDBEntries</code> and <code>closeDBHandles</code>. 
---
--- @author Sebastian Fischer
--- @version January 2011
------------------------------------------------------------------------------

module KeyDatabaseSQLite3 (

  Query, runQ,

  Transaction, TError(..), TErrorKind(..), runT, runJustT,
  (|>>=), (|>>), sequenceT, sequenceT_, mapT, mapT_,

  Dynamic, persistentSQLite3,

  existsDBKey,

  allDBKeys, allDBInfos, allDBKeyInfos,

  getDBInfo, getDBInfos,

  deleteDBEntry, deleteDBEntries, updateDBEntry, newDBEntry, cleanDB,

  closeDBHandles

  ) where

import Database     ( TError(..), TErrorKind(..) )
import Global       ( Global, GlobalSpec(..), global, readGlobal, writeGlobal )
import IO           ( Handle, hPutStrLn, hGetLine, hFlush, hClose )
import IOExts       ( connectToCommand )
import List         ( intersperse, insertBy )
import ReadNumeric  ( readInt )
import ReadShowTerm ( readQTerm, showQTerm )
import Unsafe       ( unsafePerformIO ) -- create DB and table on first access

-- adjust this if 'sqlite3' is not in the PATH
path'to'sqlite3 :: String
path'to'sqlite3 = "sqlite3"

-- Query and Transaction types

--- Queries can read but not write to the database.
data Query a = Query (IO a)

--- Runs a database query in the IO monad.
runQ :: Query a -> IO a
runQ (Query a) = a

--- Applies a function to the result of a database query.
transformQ :: (a -> b) -> Query a -> Query b
transformQ f query = Query (runQ query >>= return . f)

--- Transactions can modify the database and are executed
--- atomically.
data Transaction a = Trans (IO (TransResult a))
data TransResult a = OK a | Error TError

unTrans :: Transaction a -> IO (TransResult a)
unTrans (Trans action) = action

--- Runs a transaction atomically in the IO monad.
---
--- Transactions are <em>immediate</em>, which means that locks are
--- acquired on all databases as soon as the transaction is
--- started. After one transaction is started, no other database
--- connection will be able to write to the database or start a
--- transaction. Other connections <em>can</em> read the database
--- during a transaction of another process.
---
--- The choice to use immediate rather than deferred transactions is
--- conservative. It might also be possible to allow multiple
--- simultaneous transactions that lock tables on the first database
--- access (which is the default in SQLite). However this leads to
--- unpredictable order in which locks are taken when multiple
--- databases are involved. The current implementation fixes the
--- locking order by sorting databases by their name and locking them
--- in order immediately when a transaction begins.
---
--- More information on 
--- <a href="http://sqlite.org/lang_transaction.html">transactions</a>
--- in SQLite is available online.
---
runT :: Transaction a -> IO (Either a TError)
runT trans =
  do withAllDBHandles (`hPutAndFlush` "begin immediate")
     result <- unTrans trans
     case result of
       Error err ->
         do withAllDBHandles (`hPutAndFlush` "rollback")
            return (Right err)
       OK res ->
         do withAllDBHandles (`hPutAndFlush` "commit")
            return (Left res)

--- Like <code>runT</code> but fails on transaction errors.
runJustT :: Transaction a -> IO a
runJustT trans =
  do Left result <- runT trans
     return result

--- Lifts a database query to the transaction type such that it can be
--- composed with othe rtransactions.
getDB :: Query a -> Transaction a
getDB = transIO . runQ

-- not exported
transIO :: IO a -> Transaction a
transIO action = Trans (action >>= return . OK)

--- Returns the given value in a transaction that does not access the
--- database.
returnT :: a -> Transaction a
returnT = transIO . return

--- Returns the unit value in a transaction that does not access the
--- database. Useful to ignore results when composing transactions.
doneT :: Transaction ()
doneT = transIO done

--- Aborts a transaction with an error.
errorT :: TError -> Transaction a
errorT = Trans . return . Error

--- Aborts a transaction with a user-defined error message.
failT :: String -> Transaction a
failT = errorT . TError UserDefinedError

--- Combines two transactions into a single transaction that executes
--- both in sequence. The first transaction is executed, its result
--- passed to the function which computes the second transaction,
--- which is then executed to compute the final result.
---
--- If the first transaction is aborted with an error, the second
--- transaction is not executed.
(|>>=) :: Transaction a -> (a -> Transaction b) -> Transaction b
Trans action |>>= f = Trans $
  do result <- action
     case result of
       Error err -> return $ Error err
       OK res    -> unTrans $ f res

--- Combines two transactions to execute them in sequence. The result of
--- the first transaction is ignored.
(|>>) :: Transaction _ -> Transaction a -> Transaction a
t1 |>> t2 = t1 |>>= const t2

--- Executes a list of transactions sequentially and computes a list
--- of all results.
sequenceT :: [Transaction a] -> Transaction [a]
sequenceT = foldr seqT (returnT [])
 where
  seqT t ts = t |>>= \x -> ts |>>= \xs -> returnT (x:xs)

--- Executes a list of transactions sequentially, ignoring their
--- results.
sequenceT_ :: [Transaction _] -> Transaction ()
sequenceT_ = foldr (|>>) doneT

--- Applies a function that yields transactions to all elements of a
--- list, executes the transaction sequentially, and collects their
--- results.
mapT :: (a -> Transaction b) -> [a] -> Transaction [b]
mapT f = sequenceT . map f

--- Applies a function that yields transactions to all elements of a
--- list, executes the transactions sequentially, and ignores their
--- results.
mapT_ :: (a -> Transaction _) -> [a] -> Transaction ()
mapT_ f = sequenceT_ . map f

-- Interface based on keys

--- The name of the database file.
type DBFile = String

--- The name of the database table in which a predicate is stored.
type TableName = String

--- Result type of database predicates.
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

--- This function is used instead of <code>dynamic</code> or
--- <code>persistent</code> to declare predicates whose facts are stored
--- in an SQLite database.
---
--- If the provided database or the table do not exist they are created
--- automatically when the declared predicate is accessed for the first time.
---
--- @param dbFile - the name of a database file
--- @param tableName - the name of a database table
persistentSQLite3 :: DBFile -> TableName -> KeyPred a
persistentSQLite3 db table _ _ = unsafePerformIO $
  do ensureDBTable db table
     return $ DBInfo db table

--- Checks whether the predicate has an entry with the given key.
existsDBKey :: KeyPred _ -> Key -> Query Bool
existsDBKey keyPred key =
  transformQ (0/=) . Query . selectWhereCount keyPred $ "key = " ++ show key

--- Returns a list of all stored keys. Do not use this function unless
--- the database is small.
allDBKeys :: KeyPred _ -> Query [Key]
allDBKeys keyPred = transformQ (map readIntOrExit) $ select keyPred "key"

--- Returns a list of all info parts of stored entries. Do not use this
--- function unless the database is small.
allDBInfos :: KeyPred a -> Query [a]
allDBInfos keyPred = transformQ (map readQTerm) $ select keyPred "info"

--- Returns a list of all stored entries. Do not use this function
--- unless the database is small.
allDBKeyInfos :: KeyPred a -> Query [(Key,a)]
allDBKeyInfos keyPred =
  transformQ (map readKeyInfo) $ select keyPred "key, info"

readKeyInfo :: String -> (Key,a)
readKeyInfo row = (readIntOrExit key, readQTerm info)
 where
  key:info:_ = words row

--- Queries the information stored under the given key.
getDBInfo :: KeyPred a -> Key -> Query a
getDBInfo keyPred = transformQ readQTerm . selectKey keyPred "info"

--- Queries the information stored under the given keys.
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

--- Deletes the information stored under the given key.
deleteDBEntry :: KeyPred _ -> Key -> Transaction ()
deleteDBEntry keyPred key = transIO $
  do sqlite3 0 (dbFile keyPred) $ 
       "delete from " ++ tableName keyPred ++
       " where key = " ++ show key
     done

--- Deletes the information stored under the given keys.
deleteDBEntries :: KeyPred _ -> [Key] -> Transaction ()
deleteDBEntries keyPred keys = transIO $
  do sqlite3 0 (dbFile keyPred) $ 
       "delete from " ++ tableName keyPred ++
       " where key in (" ++ intercalate "," (map show keys) ++ ")"
     done

--- Updates the information stored under the given key.
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

--- Stores new information in the database and yields the newly
--- generated key.
newDBEntry :: KeyPred a -> a -> Transaction Key
newDBEntry keyPred info = transIO $
  do sqlite3 0 (dbFile keyPred) $
       "insert into " ++ tableName keyPred ++ " (info) " ++
       " values (" ++ quote (showQTerm info) ++ ")"
     [n] <- sqlite3 1 (dbFile keyPred) "select last_insert_rowid()"
     return $ readIntOrExit n

--- Deletes all entries from the database associated with a predicate.
cleanDB :: KeyPred _ -> Transaction ()
cleanDB keyPred = transIO $
  do sqlite3 0 (dbFile keyPred) $ "delete from " ++ tableName keyPred
     done

-- SQL query functions (not exported)

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

--- Closes all database connections. Should be called when no more
--- database queries or updates will be executed.
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


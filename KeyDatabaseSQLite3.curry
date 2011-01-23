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

  Query, runQ, transformQ, getDB,

  Transaction, TError(..), TErrorKind(..), showTError, runT, runJustT,
  returnT, doneT, errorT, failT, (|>>=), (|>>),
  sequenceT, sequenceT_, mapT, mapT_,

  Dynamic, persistentSQLite3, closeDBHandles,

  existsDBKey,

  allDBKeys, allDBInfos, allDBKeyInfos,

  getDBInfo, getDBInfos,

  deleteDBEntry, deleteDBEntries, updateDBEntry, newDBEntry, cleanDB

  ) where

import Database     ( TError(..), TErrorKind(..), showTError )
import Global       ( Global, GlobalSpec(..), global, readGlobal, writeGlobal )
import IO           ( Handle, hPutStrLn, hGetLine, hFlush, hClose, stderr )
import IOExts       ( connectToCommand )
import List         ( intersperse, insertBy )
import Maybe        ( mapMaybe )
import ReadNumeric  ( readInt )
import ReadShowTerm ( readQTerm, showQTerm, readsQTerm )

infixl 1 |>>, |>>=

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
  do beginTransaction
     result <- catchTrans $ unTrans trans
     case result of
       Error err ->
         do rollbackTransaction
            return (Right err)
       OK res ->
         do commitTransaction
            return (Left res)

catchTrans :: IO (TransResult a) -> IO (TransResult a)
catchTrans action =
  action `catch` \ (IOError msg) ->
    do err <- readGlobal lastQueryError
       writeGlobal lastQueryError Nothing
       return . Error $ maybe (TError ExecutionError msg) id err

--- Like <code>runT</code> but fails on transaction errors.
runJustT :: Transaction a -> IO a
runJustT trans =
  do Left result <- runT trans
     return result

--- Lifts a database query to the transaction type such that it can be
--- composed with other transactions. Run-time errors that occur
--- during the execution of the given query are transformed into
--- transaction errors.
getDB :: Query a -> Transaction a
getDB query = Trans . catchTrans $ runQ query >>= return . OK

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
persistentSQLite3 db table _ _ = DBInfo db table

--- Checks whether the predicate has an entry with the given key.
existsDBKey :: KeyPred _ -> Key -> Query Bool
existsDBKey keyPred key = Query $
  do n <- selectInt keyPred "count(*)" $ "where key = " ++ show key
     return $! n > 0

--- Returns a list of all stored keys. Do not use this function unless
--- the database is small.
allDBKeys :: KeyPred _ -> Query [Key]
allDBKeys keyPred = Query $
  do rows <- selectRows keyPred "key" ""
     mapIO readIntOrExit rows

--- Returns a list of all info parts of stored entries. Do not use this
--- function unless the database is small.
allDBInfos :: KeyPred a -> Query [a]
allDBInfos keyPred = Query $
  do rows <- selectRows keyPred "info" ""
     return $!! map readQTerm rows

--- Returns a list of all stored entries. Do not use this function
--- unless the database is small.
allDBKeyInfos :: KeyPred a -> Query [(Key,a)]
allDBKeyInfos keyPred = Query $
  do rows <- selectRows keyPred "*" ""
     mapIO readKeyInfo rows

readKeyInfo :: String -> IO (Key,a)
readKeyInfo row =
  do key <- readIntOrExit keyStr
     return $!! (key, readQTerm infoStr)
 where
  (keyStr,_:infoStr) = break ('|'==) row

--- Queries the information stored under the given key. Causes a
--- run-time error if the given key is not present.
getDBInfo :: KeyPred a -> Key -> Query a
getDBInfo keyPred key = Query $
  do rows <- selectRows keyPred "info" $ "where key = " ++ show key
     readHeadOrExit rows
 where
  readHeadOrExit []    = dbError KeyNotExistsError $ "getDBInfo, " ++ show key
  readHeadOrExit (x:_) = return $!! readQTerm x

--- Queries the information stored under the given keys.
getDBInfos :: KeyPred a -> [Key] -> Query [a]
getDBInfos keyPred keys = Query $
  do rows <- selectRows keyPred "*" $
               "where key in (" ++ intercalate "," (map show keys) ++ ")"
     sortByIndexInGivenList rows
 where
  sortByIndexInGivenList rows =
    do keyInfos <- mapIO readKeyInfo rows
       let err key = dbError KeyNotExistsError $ "getDBInfos, " ++ show key
       mapIO (\key -> maybe (err key) return (lookup key keyInfos)) keys

intercalate :: [a] -> [[a]] -> [a]
intercalate l = concat . intersperse l

--- Deletes the information stored under the given key. If the given
--- key does not exist this transaction is silently ignored and no
--- error is raised.
deleteDBEntry :: KeyPred _ -> Key -> Transaction ()
deleteDBEntry keyPred key =
  modify keyPred "delete from" $ "where key = " ++ show key

--- Deletes the information stored under the given keys. No error is
--- raised if (some of) the keys do not exist.
deleteDBEntries :: KeyPred _ -> [Key] -> Transaction ()
deleteDBEntries keyPred keys =
  modify keyPred "delete from" $
    "where key in (" ++ intercalate "," (map show keys) ++ ")"

--- Updates the information stored under the given key. The
--- transaction is aborted with a <code>KeyNotExistsError</code> if
--- the given key is not present in the database.
updateDBEntry :: KeyPred a -> Key -> a -> Transaction ()
updateDBEntry keyPred key info =
  errorUnlessKeyExists keyPred key ("updateDBEntry, " ++ show key) |>>
  modify keyPred "update"
    ("set info = " ++ quote (showQTerm info) ++
     " where key = " ++ show key)

errorUnlessKeyExists :: KeyPred a -> Key -> String -> Transaction ()
errorUnlessKeyExists keyPred key msg =
  getDB (existsDBKey keyPred key) |>>= \exists ->
  if not exists
    then errorT $ TError KeyNotExistsError msg
    else doneT

quote :: String -> String
quote s = "'" ++ concatMap quoteChar s ++ "'"
 where
  quoteChar c = if c == ''' then ['\\','''] else [c]

--- Stores new information in the database and yields the newly
--- generated key.
newDBEntry :: KeyPred a -> a -> Transaction Key
newDBEntry keyPred info =
  modify keyPred "insert into"
    ("(info) values (" ++ quote (showQTerm info) ++ ")") |>>
  getDB (Query $ selectInt keyPred "last_insert_rowid()" "")

--- Deletes all entries from the database associated with a predicate.
cleanDB :: KeyPred _ -> Transaction ()
cleanDB keyPred = modify keyPred "delete from" ""

-- SQL access functions

-- These functions are not exported and abstract common functionality
-- used in the library functions above. Each database access is one of
-- the following: a modification, a selection of a numeric aggregate,
-- or a selection of rows.

sqlite3 :: KeyPred _ -> String -> IO Handle
sqlite3 keyPred sql =
  do h <- getDBHandle keyPred
     hPutAndFlush h $ sql ++ ";"
     return h

hPutAndFlush :: Handle -> String -> IO ()
hPutAndFlush h s = hPutStrLn h s >> hFlush h

modify :: KeyPred _ -> String -> String -> Transaction ()
modify keyPred before after = transIO $
  do sqlite3 keyPred $
       before ++ " " ++ tableName keyPred ++ " " ++ after
     done

selectInt :: KeyPred _ -> String -> String -> IO Int
selectInt keyPred aggr cond =
  do h <- sqlite3 keyPred $
       "select distinct " ++ aggr ++
       " from " ++ tableName keyPred ++ " " ++ cond
     hGetLine h >>= readIntOrExit

-- yields 1 for "1a" and exits for ""
readIntOrExit :: String -> IO Int
readIntOrExit s = maybe err (return . fst) $ readInt s
 where
  err = dbError ExecutionError $ "readIntOrExit, " ++ show s

-- When selecting an unknown number of rows it is necessary to know
-- when to stop. One way to be able to stop is to select 'count(*)'
-- instead of the actual colums before the query. As it is potentially
-- inefficient to execute the query twice, this implementation takes a
-- different approach: generate a random string before the query and
-- select it afterwards, then read all lines up to this random string.

type Row = String

selectRows :: KeyPred _ -> String -> String -> IO [Row]
selectRows keyPred cols cond =
  do h <- sqlite3 keyPred "select hex(randomblob(8))"
     rnd <- hGetLine h -- 8 random bytes = 16 random hex chars
     hPutAndFlush h $
       "select " ++ cols ++ " from " ++ tableName keyPred ++
       " " ++ cond ++ "; select " ++ quote rnd ++ ";"
     hGetLinesBefore h rnd

hGetLinesBefore :: Handle -> String -> IO [String]
hGetLinesBefore h stop =
  do line <- hGetLine h
     if line == stop
       then return []
       else do rest <- hGetLinesBefore h stop
               return (line : rest)

--- Closes all database connections. Should be called when no more
--- database access will be necessary.
closeDBHandles :: IO ()
closeDBHandles =
  do withAllDBHandles hClose
     writeGlobal openDBHandles []

-- helper functions and globbaly stored information

dbError :: TErrorKind -> String -> IO a
dbError kind msg =
  do writeGlobal lastQueryError . Just $ TError kind msg
     error $ show kind ++ ": " ++ msg

lastQueryError :: Global (Maybe TError)
lastQueryError = global Nothing Temporary

getDBHandle :: KeyPred _ -> IO Handle
getDBHandle keyPred = 
  do ensureDBFor keyPred
     readDBHandle $ dbFile keyPred

-- Initializes the database and table for the given predicate. This
-- function must be called before the database for this predicate is
-- accessed and before a transaction that uses this predicate is
-- started.
ensureDBFor :: KeyPred _ -> IO ()
ensureDBFor keyPred =
  do ensureDBHandle db
     ensureDBTable db table
 where
  (db,table) = dbInfo keyPred

readDBHandle :: DBFile -> IO Handle
readDBHandle db = readGlobal openDBHandles >>= maybe err return . lookup db
 where
  err = dbError ExecutionError $ unlines
    ["no handle for " ++ db,
     "call ensureDBFor for the corresponding predicate"]

openDBHandles :: Global [(DBFile,Handle)]
openDBHandles = global [] Temporary

withAllDBHandles :: (Handle -> IO _) -> IO ()
withAllDBHandles f =
  do dbHandles <- readGlobal openDBHandles
     mapIO_ (f . snd) dbHandles

ensureDBHandle :: DBFile -> IO ()
ensureDBHandle db =
  do dbHandles <- readGlobal openDBHandles
     unless (db `elem` map fst dbHandles) $ addNewDBHandle dbHandles
 where
  addNewDBHandle dbHandles =
    do h <- connectToCommand $ path'to'sqlite3 ++ " " ++ db
       writeGlobal openDBHandles $ -- sort against deadlock
         insertBy ((<=) `on` fst) (db,h) dbHandles
       isTrans <- readGlobal currentlyInTransaction
       unless (not isTrans) $ hPutStrLn h "begin immediate;"

unless :: Bool -> IO () -> IO ()
unless False action = action
unless True  _      = done

on :: (b -> b -> c) -> (a -> b) -> a -> a -> c
(f `on` g) x y = f (g x) (g y)

ensureDBTable :: DBFile -> TableName -> IO ()
ensureDBTable db table =
  do dbTables <- readGlobal knownDBTables
     unless ((db,table) `elem` dbTables) $
       do h <- readDBHandle db
          hPutAndFlush h $
            "create table if not exists " ++ table ++
            " (key integer primary key asc autoincrement," ++
            "  info not null);"
          writeGlobal knownDBTables $ (db,table) : dbTables

knownDBTables :: Global [(DBFile,TableName)]
knownDBTables = global [] Temporary

beginTransaction :: IO ()
beginTransaction =
  do writeGlobal currentlyInTransaction True
     withAllDBHandles (`hPutAndFlush` "begin immediate;")

commitTransaction :: IO ()
commitTransaction =
  do withAllDBHandles (`hPutAndFlush` "commit;")
     writeGlobal currentlyInTransaction False

rollbackTransaction :: IO ()
rollbackTransaction =
  do withAllDBHandles (`hPutAndFlush` "rollback;")
     writeGlobal currentlyInTransaction False

currentlyInTransaction :: Global Bool
currentlyInTransaction = global False Temporary

-- for debugging

-- hPutStrLn h s =
--   do IO.hPutStrLn stderr $ "> " ++ s
--      IO.hPutStrLn h s

-- hGetLine h =
--   do l <- IO.hGetLine h
--      IO.hPutStrLn stderr $ "< " ++ l
--      return l

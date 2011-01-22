Shirt-sleeved SQLite binding for Curry
======================================

This is a reimplementatoin of the [`KeyDatabase`] module for the
functional logic programming language [Curry] based on SQLite. It is
shirt-sleeved because it communicates with the `sqlite3` process via
standard I/O.

[Curry]: http://www.curry-language.org/
[`KeyDatabase`]: http://www.informatik.uni-kiel.de/~pakcs/lib/CDOC/KeyDatabase.html
[SQLite]: http://sqlite.org

> SQLite is a software library that implements a self-contained,
> serverless, zero-configuration, transactional SQL database
> engine. SQLite is the most widely deployed SQL database engine in the
> world. The source code for SQLite is in the public domain.
>
> [SQLite] website

The [`KeyDatabase`] module for Curry is a database interface that
provides access to tables that consist of a _key_ and an _info_ part
via so-called persistent predicates. The tables are viewed as
relations and each row of the table defines a fact.

Persistent predicates can be used much like ordinary predicates in
logic programming. However, the `KeyDatabase` module restricts these
possibilities to those that can be easily implemented on
databases. Each predicate stores a set of key/info associations, keys
are handled by the underlying implementation and access based on keys
is fast when using a database backend like SQLite.

Compared with my previous work on this topic this module is far less
ambitious: instead of implementing arbitrary persistent predicates and
their operations, it only provides the interface of the `KeyDatabase`
module (compare with the interface of [`Dynamic`] and [`Database`] to
see what's missing) and database queries are not handles lazily but
all results are queried immediately. The latter point is not unusual
for database libraries and no severe restriction unless large query
results should be processed incrementally.

The following program shows how to use this library.

    import KeyDatabaseSQLite3
    
    hello :: Int -> String -> Dynamic
    hello = persistentSQLite3 "hello.db" "hello"
    
    main :: IO ()
    main = do
        ensureDBFor hello
        let info = "Hello, echo!"
        res <- runJustT (newDBEntry hello info |>>= getDB . getDBInfo hello)
        putStrLn res
        closeDBHandles

Compared with using `KeyDatabase` there are three main differences:

  * the import is `KeyDatabaseSQLite3`,

  * instead of `persistent`, the function `persistentSQLite3` is used
    to define the persistent predicate `hello`, and

  * the program is wrapped in calls to the functions `ensureDBFor` and
    `closeDBHandles`.

The parameters of the `persistentSQLite3` function are the names of
the database file and of the table to store the facts in,
respectively. The call to `ensureDBFor` creates the database
`hello.db` and in it the table `hello` if they do not exist yet and
`closeDBHandles` closes the handle to the `hello.db` database before
the program exits. Calling `closeDBHandles` might not be necessary if
handles are closed automatically after the program exits, but it is
useful to close a database connection earlier in case the program does
other things after accessing the database.

The above program stores the string `"Hello, echo!"` in the table
`hello` associated with the `hello` predicate, retrieves the stored
value from the database and prints it:

    # pakcs -l hello.curry
    pakcs> main
    Hello, echo!
    pakcs> main
    Hello, echo!
    pakcs> :q

We can check using the `sqlite3` program that indeed the value is
stored in the database.

    # sqlite3 hello.db
    sqlite> select * from hello;
    1|"Hello, echo!"
    2|"Hello, echo!"
    sqlite> .q

The value is stored twice under different keys, because we have
executed `main` twice.

The values are stored in the database using the a string
representation that can be parsed using the [`readQTerm`] function.

[`readQTerm`]: http://www.informatik.uni-kiel.de/~pakcs/lib/CDOC/ReadShowTerm.html#readQTerm

If you want to try this library yourself, you need to install the
Curry system [PAKCS] and the `sqlite3` command line program. This
module comes with tests that can be executed by running

    # pakcs -1 -r tests.curry

in the directory where this library and the test file reside. You can generate HTML documentation for the module by calling:

    # currydoc --html doc KeyDatabaseSQLite3.curry

[PAKCS]: http://www.informatik.uni-kiel.de/~pakcs/download/

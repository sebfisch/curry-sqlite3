Changes in curry-sqlite3
========================

0.4
---

  * tuples can be stored in multiple columns
  * `persistentSQLite` has an argument for column names

0.3
---

This version uses a database format that is incompatible with previous
versions and the names in the interface don't mention SQLite's
version.

  * keys are not represented explicitly in the database
  * module was renamed to `KeyDatabaseSQLite`
  * `persistentSQLite3` was renamed to `persistentSQLite`

0.2
---

  * removed `ensureDBFor` function in favor of implicit initialization
  * added Makefile for documentation and tests
  * added changelog

0.1
---

  * initial implementation of KeyDatabase interface based on SQLite

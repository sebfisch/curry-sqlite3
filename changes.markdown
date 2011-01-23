Changes in curry-sqlite3
========================

0.3
---

This version uses a database format that is incompatible with previous
versions.

  * keys are not represented explicitly in the database
  * tuples can be stored in multiple columns
  * renamed module to `KeyDatabaseSQLite`
  * renamed `persistentSQLite3` to `persistentSQLite`
    and added argument for column names
  * fixed bug in `quote` function

0.2
---

  * removed `ensureDBFor` function in favor of implicit initialization
  * added Makefile for documentation and tests
  * added changelog

0.1
---

  * initial implementation of KeyDatabase interface based on SQLite

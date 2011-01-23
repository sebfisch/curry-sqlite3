Changes in curry-sqlite3
========================

0.3
---

This version uses a database format that is incompatible with previous
versions and the names in the inface don't mention SQLite's version.

  * keys are not represented explicitly in the database
  * module was to `KeyDatabaseSQLite`
  * `persistentSQLite3` was renamed to `persistentSQLite`

0.2
---

  * removed `ensureDBFor` function in favor of implicit initialization
  * added Makefile for documentation and tests
  * added changelog

0.1
---

  * initial implementation of KeyDatabase interface based on SQLite

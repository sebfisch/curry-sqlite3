doc/KeyDatabaseSQLite.html: KeyDatabaseSQLite.curry
	currydoc --html doc $<

.PHONY tests:
	pakcs -q -r tests.curry

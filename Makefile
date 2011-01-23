doc/KeyDatabaseSQLite3.html: KeyDatabaseSQLite3.curry
	currydoc --html doc $<

.PHONY tests:
	pakcs -q -r tests.curry

Interactive SQL Shell for Guile
===============================

This tool is an interactive SQL shell based on Guile DBI.

Pre-requisites
--------------

Guile  
guile-dbi from [here](https://github.com/opencog/guile-dbi)

Usage
-----

guile isql.scm  
Enter ":?" for help.

To connect to a database, enter:  
connect <dbtype> <guile dbi database connection string>  
for example  
connect sqlite3 mydb.db  
connect postgres username:password:database:tcp:5432  
connect mysql username:password:database:tcp:3306  

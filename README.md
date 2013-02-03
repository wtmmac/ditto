Ditto
============================================

Ditto is a tool that lets you easily replicate your existing MySQL data into
MemSQL.

Dependencies
============
Ditto requires the following software:

* MySQL &lt;= 5.5 (5.6 is not supported)
* Python 2.7
* MemSQL
* MySQL-Python
* mysqldump


Installation
=============

The replication scripts come bundled with a modified version of the
[python-mysql-replication](https://github.com/noplay/python-mysql-replication)
library that must be installed before we can replicate.

Enter the python-mysql-replication directory:

    $ python setup.py install 

MySQL server settings
=========================

In your MySQL server configuration file (usually at /etc/mysql/my.cnf) you need
to enable replication. Make sure the following options are set as below:

    [mysqld]
    server-id		 = 1
    log_bin			 = /var/log/mysql/mysql-bin.log
    expire_logs_days = 10
    max_binlog_size  = 100M
    binlog-format    = row

Usage
=====

The replication script is ``replicate.py``. To run the script,
enter the scripts directory:
    
    $ python replicate.py [database]

This script will recreate the specified database in MemSQL. By default, it
first runs ``mysqldump`` on the database and flushes the binlog. Then it waits
for queries on the current binlog that pertain to the specified database and
runs them on MemSQL. To stop the program, send it an interrupt. There are a
number of settings you can tweak at the command line:

    $ python replicate.py --help

    usage: replicate.py [-h] [--host HOST] [--user USER] [--password PASSWORD]
                        [--mysql-port MYSQL_PORT] [--memsql-port MEMSQL_PORT]
                        [--sql_mode SQL_MODE] [--no-dump] [--no-flush]
                        database

    Replicate a MySQL database to MemSQL

    positional arguments:
    database              Database to use

    optional arguments:
    -h, --help            show this help message and exit
    --host HOST           Host where the database server is located
    --user USER           Username to log in as
    --password PASSWORD   Password to use
    --mysql-port MYSQL_PORT
                            MySQL port to use
    --memsql-port MEMSQL_PORT
                            MemSQL port to use
    --sql_mode SQL_MODE   SQL_MODE to use
    --no-dump             Don't run mysqldump before reading (expects schema to
                            already be set up)
    --no-flush            Don't flush the binlog before reading (may duplicate
                            existing data)

The scripts directory also contains ``test_replication.py``. This script will
replicate a specific database from MySQL to MemSQL using mysqldump and the
current binlog and then verify that the content of all tables in the MySQL
database match those in the newly created MemSQL database. It can be run in the
same manner as ``replicate.py``, though it is not intended for replication, as
it doesn't wait for new queries on the current binlog. To print out all queries
in the current binlog, run the ``dump_queries.py`` script.

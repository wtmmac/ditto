Ditto
============================================

Ditto is a tool that lets you easily replicate your existing MySQL data into
MemSQL.

Dependencies
============
Ditto depends on the following software:

* MySQL 5.5 (5.6 is not supported)
* Python 2.7
* MemSQL
* MySQL-Python


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

The replication script is ``mysql_memsql_replication.py``. To run the script,
enter the scripts directory:
    
    $ python mysql_memsql_replication.py

This script looks at the current MySQL binlog (with the ``SHOW MASTER STATUS``
command). It then reads all of the queries from the binlog and executes them on
an instance of MemSQL. There are a number of settings you can tweak at the
command line:

    $ python mysql_memsql_replication.py --help
    ==> usage: mysql_memsql_replication.py [-h] [--host HOST] [--user USER]
                                           [--passwd PASSWD] [--db DB]
                                           [--mysql-port MYSQL_PORT]
                                           [--memsql-port MEMSQL_PORT]
                                           [--sql_mode SQL_MODE]
     
        Replicate MySQL to MemSQL
        
        optional arguments:
            -h, --help            show this help message and exit
            --host HOST           Host where the database server is located
            --user USER           Username to log in as
            --passwd PASSWD       Password to use
            --db DB               Database to use
            --mysql-port MYSQL_PORT
                                  MySQL port to use
            --memsql-port MEMSQL_PORT
                                  MemSQL port to use
            --sql_mode SQL_MODE   SQL_MODE to use

The scripts directory also contains ``test_replication.py``. This script will
replicate a specific database from MySQL to MemSQL using the current binlog and
then verify that the content of all tables in the MySQL database match those in
the newly created MemSQL database. It can be run in the same manner as
``mysql_memsql_replication.py``, however a database must be specified with the
``--db`` flag. To print out all queries in the binlog, run the
``dump_events.py`` script.

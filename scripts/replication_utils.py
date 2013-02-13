import MySQLdb
import MySQLdb.converters
import memsql_database

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import *
from pymysqlreplication.event import *

import argparse
import datetime
import subprocess
import os
import binascii

def fix_object(value):

    """Fixes python objects so that they can be properly inserted into SQL queries"""

    # Needs to turn it into a regular string, since MySQLdb doesn't escape
    # unicode values properly
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value

def compare_items((k, v)):
    """Converta a column-value pair to an equality comparison (uses IS for NULL)"""
    if v == None:
        return '`%s` IS %%s'%k
    else:
        return '`%s`=%%s'%k

def parse_commandline():
        """Parses the commandline-arguments that one could enter to a script replicating MySQL to MemSQL"""

        parser = argparse.ArgumentParser(description='Replicate a MySQL database to MemSQL')
        parser.add_argument('database', help='Database to use')
        parser.add_argument('--host', dest='host', type=str, help='Host where the database server is located', default='127.0.0.1')
        parser.add_argument('--user', dest='user', type=str, help='Username to log in as', default='root')
        parser.add_argument('--password', dest='password', type=str, help='Password to use', default='')
        parser.add_argument('--mysql-port', dest='mysql_port', type=int, help='MySQL port to use', default=3307)
        parser.add_argument('--memsql-port', dest='memsql_port', type=int, help='MemSQL port to use', default=3306)
        parser.add_argument('--no-dump', dest='no_dump', action='store_true',
                default=False, help="Don't run mysqldump before reading (expects schema to already be set up)")
        parser.add_argument('--no-flush', dest='no_flush', action='store_true',
                default=False, help="Don't flush the binlog before reading (may duplicate existing data)")

        args = parser.parse_args()
        return args

def get_mysql_settings(args):
    return {'host':args.host, 'user':args.user, 'passwd':args.password,
            'db': args.database, 'port':args.mysql_port}

def get_memsql_settings(args):
    return {'host': args.host+':'+str(args.memsql_port), 'user': args.user,
            'database':args.database, 'password': args.password}

def connect_to_mysql_stream(args, blocking=True):
    """Returns an iterator through the latest MySQL binlog

    Expects that the `args' argument was obtained from the
    parse_commandline() function (or something very similar)
    """

    mysql_settings = get_mysql_settings(args)

    ##server_id is your slave identifier. It should be unique
    ##blocking: True if you want to block and wait for the next event at the end of the stream
    server_id = int(binascii.hexlify(os.urandom(4)), 16) # A random 4-byte int
    stream = BinLogStreamReader(connection_settings = mysql_settings,
                    server_id = server_id, blocking = blocking, only_events =
                    [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent])

    return stream

def connect_to_memsql(args):
    """Connects to a MemSQL instance to replicate to

    Expects that the `args' argument was obtained from the
    parse_commandline() function (or something very similar)
    """

    # Dumps database and flushes logs based on flags
    mysql_settings = get_mysql_settings(args)
    if not args.no_dump:
        # Dump with mysqldump
        dumpcommand = ['mysqldump', '--user='+args.user, '--host='+args.host,
            '--port='+str(args.mysql_port), '--database', args.database, '--force']
        if args.password:
            dumpcommand.append('--password='+args.password)
        if not args.no_flush:
            dumpcommand.append('--flush-logs')
        print 'executing: {0}'.format(' '.join(dumpcommand))
        p = subprocess.Popen(dumpcommand, stdout=subprocess.PIPE)
        dump = p.communicate()[0]

        # Run mysql client (connected to memsql) on file
        mysqlcommand = ['mysql', '--user='+args.user, '--host='+args.host,
            '--port='+str(args.memsql_port), '--force']
        if args.password:
            mysqlcommand.append('--password='+args.password)

        print 'executing: {0}'.format(' '.join(mysqlcommand))
        p = subprocess.Popen(mysqlcommand, stdin=subprocess.PIPE)
        p.communicate(input=dump)

    elif not args.no_flush:
        print 'flushing binlogs'
        MySQLdb.connect(**mysql_settings).cursor().execute('FLUSH LOGS')

    memsql_settings = get_memsql_settings(args)
    memsql_conn = memsql_database.Connection(**memsql_settings)

    return memsql_conn

def process_binlogevent(binlogevent):
        """Extracts the query/queries from the given binlogevent"""

        # Each query is a pair with a string and a list of parameters for
        # format specifiers
        queries = []

        if isinstance(binlogevent, QueryEvent):
            if binlogevent.query != 'BEGIN': # BEGIN events don't matter
                queries.append( (binlogevent.query, []) )
        else:
            for row in binlogevent.rows:
                if isinstance(binlogevent, WriteRowsEvent):
                    query = ('INSERT INTO {0}({1}) VALUES ({2})'.format(
                                binlogevent.table,
                                ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
                                ', '.join(['%s'] * len(row['values']))
                                ),
                                map(fix_object, row['values'].values())
                            )
                elif isinstance(binlogevent, DeleteRowsEvent):
                    query = ('DELETE FROM {0} WHERE {1} LIMIT 1'.format(
                                binlogevent.table,
                                ' AND '.join(map(compare_items, row['values'].items()))
                                ),
                                map(fix_object, row['values'].values())
                            )
                elif isinstance(binlogevent, UpdateRowsEvent):
                    query = ('UPDATE {0} SET {1} WHERE {2} LIMIT 1'.format(
                                binlogevent.table,
                                ', '.join(['`%s`=%%s'%k for k in row['after_values'].keys()]),
                                ' AND '.join(map(compare_items, row['before_values'].items()))
                                ),
                                map(fix_object, row['after_values'].values() + row['before_values'].values())
                            )
                queries.append(query) # It should never be the case that query wasn't created

        return queries


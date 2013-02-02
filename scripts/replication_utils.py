import MySQLdb
import MySQLdb.converters
import memsql_database

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import *
from pymysqlreplication.event import *

import argparse
import datetime
import pytz
import os

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
                return str(k) + ' IS %s'
        else:
                return str(k) + '= %s'

def parse_commandline():
        """Parses the commandline-arguments that one could enter to a script replicating MySQL to MemSQL"""

        parser = argparse.ArgumentParser(description='Replicate MySQL to MemSQL')
        parser.add_argument('--host', dest='host', type=str, help='Host where the database server is located', default='127.0.0.1')
        parser.add_argument('--user', dest='user', type=str, help='Username to log in as', default='root')
        parser.add_argument('--passwd', dest='passwd', type=str, help='Password to use', default='')
        parser.add_argument('--db', dest='db', type=str, help='Database to use', default='')
        parser.add_argument('--mysql-port', dest='mysql_port', type=int, help='MySQL port to use', default=3307)
        parser.add_argument('--memsql-port', dest='memsql_port', type=int, help='MemSQL port to use', default=3306)
        parser.add_argument('--sql_mode', dest='sql_mode', type=str, help='SQL_MODE to use', default=None)

        return parser.parse_args()

def connect_to_mysql_stream(args):
        """Returns an iterator through the latest MySQL binlog

        Expects that the `args' argument was obtained from the
        parse_commandline() function (or something very similar)
        """

        mysql_settings = dict((k, v) for (k, v) in vars(args).items() if v) # Removes all blank values
        mysql_settings.pop('memsql_port')
        mysql_settings['port'] = mysql_settings.pop('mysql_port') # Changes mysql-port (always has a value) key to port
        ##server_id is your slave identifier. It should be unique
        ##blocking: True if you want to block and wait for the next event at the end of the stream

        stream = BinLogStreamReader(connection_settings = mysql_settings,
                        server_id = 3, blocking = False, only_events =
                        [DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, QueryEvent])

        return stream

def connect_to_memsql(args):
        """Connects to a MemSQL instance to replicate to

        Expects that the `args' argument was obtained from the
        parse_commandline() function (or something very similar)
        """

        memsql_settings = {'host': args.host+':'+str(args.memsql_port), 'user': args.user,
                'database': '', 'password': args.passwd}
        memsql_conn = memsql_database.Connection(**memsql_settings)

        # If a database was specified, it creates the database in MemSQL
        # if it doesn't exist. Otherwise, it creates and uses the database
        # mysql_replicated
        if args.db:
            memsql_conn.execute('CREATE DATABASE IF NOT EXISTS ' + args.db)
            memsql_conn.execute('USE ' + args.db)
        else:
            memsql_conn.execute('CREATE DATABASE IF NOT EXISTS mysql_replicated')
            memsql_conn.execute('USE mysql_replicated')

        # We need to set the timezone to GMT to prevent python from change
        # datetime values
        os.environ['TZ'] = '0'

        return memsql_conn

def process_binlogevent(binlogevent):
        """Extracts the query/queries from the given binlogevent"""

        # Each query is a pair with a string and a list of parameters for
        # format specifiers
        queries = []

        if isinstance(binlogevent, QueryEvent):
            if binlogevent.query != 'BEGIN': # BEGIN events don't matter
                queries.append( (binlogevent.query + ';', []) )
        else:
            for row in binlogevent.rows:
                if isinstance(binlogevent, WriteRowsEvent):
                    query = ('INSERT INTO {0}({1}) VALUES ({2});'.format(
                                binlogevent.table,
                                ', '.join(map(str, row['values'].keys())),
                                ', '.join(['%s'] * len(row['values']))
                                ),
                                map(fix_object, row['values'].values())
                            )
                elif isinstance(binlogevent, DeleteRowsEvent):
                    query = ('DELETE FROM {0} WHERE {1};'.format(
                                binlogevent.table,
                                ' AND '.join(map(compare_items, row['values'].items()))
                                ),
                                map(fix_object, row['values'].values())
                            )
                elif isinstance(binlogevent, UpdateRowsEvent):
                    query = ('UPDATE {0} SET {1} WHERE {2};'.format(
                                binlogevent.table,
                                ', '.join([str(k)+'=%s' for k in row['after_values'].keys()]),
                                ' AND '.join(map(compare_items, row['before_values'].items()))
                                ),
                                map(fix_object, row['after_values'].values() + row['before_values'].values())
                            )
                queries.append(query) # It should never be the case that query wasn't created

        return queries


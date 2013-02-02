# Prints out all the SQL queries in the current binlog
from replication_utils import *
import pymysql

args = parse_commandline()
stream = connect_to_mysql_stream(args)
# Connects to a dummy MySQL instance to escape query parameters (without having
# to execute them)
mysql_settings = get_mysql_settings(args)
mysql_settings['charset'] = 'utf8'
dummy_conn = pymysql.connect(**mysql_settings)

for binlogevent in stream:
    queries = process_binlogevent(binlogevent)
    for q in queries:
        query_args = map(lambda obj: dummy_conn.escape(obj), q[1])
        print q[0] % tuple(query_args)

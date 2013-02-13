from replication_utils import *

args = parse_commandline()

# Connects to MySQL and MemSQL
stream = connect_to_mysql_stream(args)
memsql_conn = connect_to_memsql(args)
memsql_conn.print_queries = True

print 'listening'

try:
    # Reads the MySQL binlog and executes the retrieved queries in MemSQL
    for binlogevent in stream:
            queries = process_binlogevent(binlogevent)
            for q in queries:
                    print q[0], q[1]
                    try:
                            memsql_conn.execute(q[0], *q[1])
                    except Exception as e:
                            print 'error:', e
except KeyboardInterrupt:
    print '\nExiting'
    stream.close()

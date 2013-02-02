# This script replicates the current MySQL binlog of a specific database into
# MemSQL and makes sure that the tables in the specified database match

from replication_utils import *
import memsql_database

args = parse_commandline()

# Since we are comparing results from a specific database, the user must
# specify a database
if not args.db:
    print 'A database must be specified with the --db flag'
    sys.exit(1)

stream = connect_to_mysql_stream(args)
memsql_conn = connect_to_memsql(args)

# Resets the database in memsql -- just in case the database isn't empty,
# so we only look at tables that were created in this run of the program
memsql_conn.execute('DROP DATABASE IF EXISTS ' + args.db)
memsql_conn.execute('CREATE DATABASE ' + args.db)
memsql_conn.execute('USE ' + args.db)

# Reads the binlog and executes the retrieved queries in MemSQL
for binlogevent in stream:
        queries = process_binlogevent(binlogevent)
        # Runs the queries in MemSQL
        for q in queries:
                print q[0], q[1]
                try:
                        memsql_conn.execute(q[0], *q[1])
                except Exception as e:
                        print 'error:', e
stream.close()

# Compares the MySQL data to the MemSQL data
mysql_settings = {'host': args.host+':'+str(args.mysql_port), 'user': args.user,
        'database': args.db, 'password': args.passwd}
mysql_conn = memsql_database.Connection(**mysql_settings)

tables = []
for row in mysql_conn.query('show tables'):
    tables.extend(row.values())

for t in tables:
    print 'Testing table', t
    try:
        memsql_database.compare_assert(mysql_conn, memsql_conn, 'select * from ' + t, enforce_order=False)
    except AssertionError as e:
        print 'AssertionError:', e

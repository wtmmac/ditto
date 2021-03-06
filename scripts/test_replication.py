# Copyright 2013 MemSQL, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License.  You may obtain a copy of the
# License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations under the License.

# This script replicates the current MySQL binlog of a specific database into
# MemSQL and makes sure that the tables in the specified database match

from replication_utils import *
import memsql_database
import sys

args = parse_commandline()

stream = connect_to_mysql_stream(args, blocking=False)
memsql_conn = connect_to_memsql(args)

memsql_conn.set_print_queries(True)

# Reads the binlog and executes the retrieved queries in MemSQL
for binlogevent in stream:
        queries = process_binlogevent(binlogevent)
        # Runs the queries in MemSQL
        for q in queries:
                try:
                    memsql_conn.execute(q[0], *q[1])
                except Exception as e:
                        print 'error:', e
stream.close()

# Compares the MySQL data to the MemSQL data
mysql_settings = {'host': args.host+':'+str(args.mysql_port), 'user': args.user,
        'database': args.database, 'password': args.password}
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

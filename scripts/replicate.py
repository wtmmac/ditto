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

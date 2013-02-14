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

print 'listening'

try:
    for binlogevent in stream:
        queries = process_binlogevent(binlogevent)
        for q in queries:
            query_args = map(lambda obj: dummy_conn.escape(obj), q[1])
            print q[0] % tuple(query_args)
except KeyboardInterrupt:
    print '\nExiting'

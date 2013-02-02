#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A lightweight wrapper around _mysql."""

import copy
import _mysql
import itertools
import logging
import time
import sys
import collections

class Connection(object):
    """A lightweight wrapper around _mysql DB-API connections.

    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage::

        db = database.Connection("localhost", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title

    Cursors are hidden by the implementation, but other than that, the methods
    are very similar to the DB-API.

    We explicitly set character encoding to UTF-8 on all connections to avoid
    encoding errors.
    """
    def __init__(self, host, database, user=None, password=None,
                 max_idle_time=7*3600, convert=False, isMySQL = False):
        self.host = host
        self.database = database
        self.max_idle_time = max_idle_time
        self.print_queries = False
        self.print_results = False

        sys_vars = dict(
                character_set_server =  "utf8",
                collation_server =      "utf8_general_ci",
                )
        args = dict(db=database)

        from MySQLdb.converters import conversions
        
        if convert:
            args["conv"] = conversions
        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        if host is None:
            pass
        elif "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306
        if isMySQL:
            # memsql does not recognize this variable
            sys_vars["storage_engine"] = "InnoDB"

        args["init_command"] = 'set names "utf8" collate "utf8_bin"' + ''.join([', @@%s = "%s"' % t for t in sys_vars.items()])

        self._db = None
        self._db_args = args
        self.encoders = dict([ (k, v) for k, v in conversions.items()
                               if type(k) is not int ])
        self._last_use_time = time.time()
        self.reconnect()
        self._db.set_character_set("utf8")

    def __del__(self):
        self.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.close()

    def set_print_queries(self, print_queries):
        self.print_queries = print_queries

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        conn = _mysql.connect(**self._db_args)
        if conn is not None:
            self.close()
            self._db = conn

    def version(self):
        return self._db.get_server_info()

    def query(self, query, *parameters):
        """
        Query the connection and return the rows (or affected rows if not a
        select query).  Mysql errors will be propogated as exceptions.
        """
        self._execute(query, *parameters)
        self._result = self._db.use_result()
        if self._result is None:
            return self._rowcount
        fields = zip(*self._result.describe())[0]
        rows = list(self._result.fetch_row(0))
        ret = SelectResult(fields, rows)
        if self.print_results:
            print ret;
        return ret;

    def query_swallow(self, query, *parameters):
        try:
            return self.query(query, *parameters)
        except MySQLError as e:
            return e

    def query_retry(self, allowed_error_fn, query, *parameters):
        max_time=60
        sleep_time=0.1

        iters = int(max_time / sleep_time)

        last_error = None
        for i in range(iters):
            try:
                ret = self.query(query, *parameters)
                done = True
                return ret
            except MySQLError as e:
                last_error = e
                if allowed_error_fn(e):
                    time.sleep(sleep_time)
                else:
                    raise
        assert last_error is not None
        raise last_error

    def affected_rows(self):
        return self._rowcount

    def mysql_info(self):
        return self._db.info()

    def get(self, query, *parameters):
        """Returns the first row returned for the given query."""
        rows = self.query(query, *parameters)
        if not rows:
            return None
        elif not isinstance(rows, list):
            raise Exception("Query is not a select query")
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]


    def assert_failure(self, code, query, *parameters):
        try:
            self.query(query, *parameters)
            assert False, "Query [%s] was expected to fail" % query
        except MySQLError as (n, m):
            if code:
                assert n == code, \
                    "[%s] returned unexpected error: (%d, %s). Expected %d"  \
                        % (query, n, m, code)
    
    def assert_lockdown(self, query, *parameters):
        self.assert_failure(1707, query, *parameters)

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *parameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters)

    def execute_lastrowid(self, query, *parameters):
        """Executes the given query, returning the lastrowid from the query."""
        self._execute(query, *parameters)
        self._result = self._db.store_result()
        return self._db.insert_id()

    def execute_rowcount(self, query, *parameters):
        """Executes the given query, returning the rowcount from the query."""
        self._execute(query, *parameters)
        self._result = self._db.store_result()
        return self._result.num_rows()

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None or
            (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _execute(self, query, *parameters):
        if parameters != None and parameters != ():
            query = query % self._db.escape(parameters, self.encoders)
        if self.print_queries:
            print "%s;" % query
        if isinstance(query, unicode):
            query = query.encode(self._db.character_set_name())

        self._db.query(query)
        self._rowcount = self._db.affected_rows()
        
    def databases(self, metadata=False):
        ignore = ["information_schema", "memsql", "mysql"] if metadata==False else []
        return [row["Database"] for row in self.query("show databases") if row["Database"] not in ignore]

from collections import OrderedDict
class Row(OrderedDict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

class SelectResult(list):
    """ Goal: create a container to hold a sql result that doesn't lose any
        information, but is compatable with our current scripts """
    def __init__(self, fieldnames, values, format="dict"):
        self.fieldnames = fieldnames
        self.values = values
        self.format = format

    def __iter__(self):
        if self.format == "dict":
            return iter(self.old_format())
        else:
            return iter(self.values)
    
    def __len__(self):
        return len(self.values)
    
    def width(self):
        return len(self.fieldnames)
    
    def __getitem__(self, rowId):
        if isinstance(rowId, slice):
            return SelectResult(self.fieldnames, self.values[rowId], self.format);
        elif self.format == "dict":
            return Row(zip(self.fieldnames, self.values[rowId]))
        else:
            return self.values[rowId]
    
    def __eq__(self, other):
        # don't use isinstance here because this class inherits list
        if type(other)==list:
            # remain compatible with old tests
            return other == self.old_format()
        return results_equal(self, other, True)
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __str__(self):
        return str(self.old_format())
    
    def sort(self):
        self.values.sort()
    
    def old_format(self):
        return [Row(itertools.izip(self.fieldnames, row)) for row in self.values]
    
    def filter_columns(self, keys):
        ikeys = [key for key in keys if isinstance(key, int)]
        skeys = [key for key in keys if isinstance(key, str)]
        selection = [i for i in range(self.width()) if i in ikeys or fieldnames[i] in skeys]
        return SelectResult([self.fieldnames[i] for i in selection],
                [[value[i] for i in selection] for value in self.values], self.format)
    
    def set_format(self, format):
        self.format = format
    
    def format_column(self, value):
        if value is None:
            return "NULL"
        if type(value) == "date":
            return value.isoformat()
        if type(value) == "datetime":
            return value.isoformat()
        return str(value)

    def format_assoc(self):
        return [", ".join(["%s:%s" % (col[0], self.format_column(col[1])) for col in zip(self.fieldnames, row)]) for row in self.values]

    def format_table(self, return_list=False):
        if len(self) == 0:
            ret = ["Empty Set"]
        else:
            values = [[self.format_column(column) for column in row] for row in self.values]
            widths = [max(len(self.fieldnames[i]), max([len(row[i]) for row in values])) for i in xrange(len(self.fieldnames))]

            separator = '+' + ''.join(['-' * (width+2) + '+' for width in widths])
            format_string  = "| " + " | ".join(["{%d:%d}" % (i, widths[i]) 
                                        for i in range(len(widths))]) + " |"
            footer = "%d row%s in set" % (len(values), "" if len(values) == 1 else "s")

            ret  = [separator]
            ret += [format_string.format(*self.fieldnames)]
            ret += [separator]
            ret += [format_string.format(*row) for row in values]
            ret += [separator]
            ret += [footer]

        if return_list:
            return ret
        return '\n'.join(ret)

def results_equal(r1, r2, enforce_order=True):
    if type(r1) != type(r2):
        return False
    if isinstance(r1, SelectResult):
        if len(r1) != len(r2):
            return False
        if r1.fieldnames != r2.fieldnames:
            return False
        if enforce_order:
            return r1.values == r2.values
        else:
            return collections.Counter(r1.values) == collections.Counter(r2.values)
    return r1 == r2

# Alias some common MySQL exceptions
IntegrityError = _mysql.IntegrityError
OperationalError = _mysql.OperationalError
MySQLError = _mysql.MySQLError
ProgrammingError = _mysql.ProgrammingError

# Convenience functions to get a MemSQL or MySQL Connection with the right
# hostname, port, and database.
class MemSQLConnection(Connection):
    def __init__(self, host='127.0.0.1:3306', user='root', database='', **kwargs):
        Connection.__init__(self, host=host, user=user, database=database, **kwargs)

class MySQLConnection(Connection):
    def __init__(self, host='127.0.0.1:3307', user='root', database='', **kwargs):
        Connection.__init__(self, host=host, user=user, database=database, **kwargs)

def compare_result(c1_result, c2_result, enforce_order=True):
    if results_equal(c1_result, c2_result, enforce_order):
        return c1_result
    else:
        assert False, "[%s] vs [%s] " % (c1_result, c2_result)

def compare_assert(c1, c2, query, allow_errors=True, enforce_order=True):
    try:
        c1_n = 0
        c2_n = 0

        c1_m = ''
        c2_m = ''

        try:
            c1_result = c1.query(query)
        except MySQLError as (c1_n, c1_m):
            pass

        try:
            c2_result = c2.query(query)
        except MySQLError as (c2_n, c2_m):
            pass

        if c1_n != 0 or c2_n != 0:
            if c1_n != c2_n:
                assert False, "(%d, %s) vs (%d, %s)" % (c1_n, c1_m, c2_n, c2_m)
            elif not allow_errors:
                raise MySQLError(c1_n, c1_m)
            else:
                return (c1_n, c1_m)

        return compare_result(c1_result, c2_result, enforce_order=enforce_order)
    except AssertionError as e:
        assert False, "Query[%s] Failure[%s]" % (query, e)

def pcompare_assert(c1, c2, *queries):
    for query in queries:
        print "%s;" % query
        print compare_assert(c1, c2, query), "\n"


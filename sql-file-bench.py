#!/usr/bin/env python
#
#    sql-file-bench.py is a benchmark utility which takes a text file
#    of sql queries one per line and issues them against the target database
#
#    Copyright (c) 2012, PostgreSQL, Experts, Inc.
#
#    Permission to use, copy, modify, and distribute this software and its
#    documentation for any purpose, without fee, and without a written
#    agreement is hereby granted, provided that the above copyright notice and
#    this paragraph and the following two paragraphs appear in all copies.
#
#    IN NO EVENT SHALL POSTGRESQL EXPERTS, INC. BE LIABLE TO ANY PARTY FOR
#    DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
#    LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
#    DOCUMENTATION, EVEN IF POSTGRESQL EXPERTS, INC. HAS BEEN ADVISED OF THE
#    POSSIBILITY OF SUCH DAMAGE.
#
#    POSTGRESQL EXPERTS, INC. SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING,
#    BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
#    FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS"
#    BASIS, AND POSTGRESQL EXPERTS, INC. HAS NO OBLIGATIONS TO PROVIDE
#    MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

import psycopg2
import os
from optparse import OptionParser
import re

parser = OptionParser()

parser = OptionParser()

parser.add_option('-d', "--dbname", action="store", type="string",
                  dest="dbname")

parser.add_option('', "--pghost", action="store", type="string",
                  dest="pghost")
parser.add_option('-U', "--pguser", action="store", type="string",
                  dest="pguser")
parser.add_option('-p', "--port", action="store", type="string",
                  dest="pgport")
parser.add_option('-f', "--file", action="store", type="string",
                  dest="file")
parser.add_option('', "--debug", action="store_const", const=1,
                  dest="debug", default=0)
parser.add_option('-l', "--loops", action="store", type="int",
                  dest="loops", default=1)


(options, args) = parser.parse_args()

# set up some environment variables either from the command line arguments,
# from the environment or from reasonable defaults

DBHOST = options.pghost or os.getenv("PGHOST") or None

DBPORT = options.pgport or os.getenv("PGPORT") or "5432"
DBUSER = options.pguser or os.getenv("PGUSER") or "postgres"
DBNAME = options.dbname or os.getenv("PGDATABASE") or "postgres"

connection_string = "dbname=%(dbname)s user=%(dbuser)s" % { 'dbname': DBNAME, 'dbuser': DBUSER }

if DBHOST is not None:
    connection_string = connection_string + " host=%(dbhost)s" % { 'dbhost': DBHOST }

if DBPORT is not None:
    connection_string = connection_string + " port=%(dbport)s" % { 'dbport': DBPORT }

try:
  db = psycopg2.connect(connection_string)
  db.autocommit = True
except:
  print "connection failed!"

cur = db.cursor()

def bench(inputfile=None):
  writes = re.compile('^\s*(insert|update|delete)',flags=re.IGNORECASE)
  for i in range(0, options.loops):
    file = open(inputfile, "r")
    for sql in file:
      if options.debug:
        print sql
      if not writes.match(sql):
        cur.execute(sql)
        results = cur.fetchall()
      if options.debug:
      	print results


if __name__ == "__main__":
  bench(inputfile=options.file)
#!/usr/bin/env python
#
#    pg_redis_bench.py is licensed under the PostgreSQL License:
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
import redis
import cPickle
import hashlib
from optparse import OptionParser

parser = OptionParser()

parser.add_option('-d', "--dbname", action="store", type="string",
                  dest="dbname")

parser.add_option('', "--pghost", action="store", type="string",
                  dest="pghost")
parser.add_option('-U', "--pguser", action="store", type="string",
                  dest="pguser")
parser.add_option('-p', "--port", action="store", type="string",
                  dest="pgport")
parser.add_option('-s', "--sql", action="store", type="string",
                  dest="sql")
parser.add_option('', "--debug", action="store_const", const=1,
                  dest="debug", default=0)
parser.add_option('', "--redis", action="store_const", const=1,
                  dest="redis", default=0)
parser.add_option('-l', "--loops", action="store", type="int",
                  dest="loops", default=50000)


(options, args) = parser.parse_args()

# set up some environment variables either from the command line arguments,
# from the environment or from reasonable defaults

DBHOST = options.pghost or os.getenv("PGHOST") or None

DBPORT = options.pgport or os.getenv("PGPORT") or "5432"
DBUSER = options.pguser or os.getenv("PGUSER") or "postgres"
DBNAME = options.dbname or os.getenv("PGDATABASE") or "postgres"
sql = options.sql or "SELECT version()"
loops = options.loops


# Redis Object
R_SERVER = redis.Redis("localhost")

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

# cache_redis module based on http://clasense4.wordpress.com/2012/07/29/python-redis-how-to-cache-python-mysql-result-using-redis/
def cache_redis(sql, TTL = 3600):

    # Create a hash key
    hash = hashlib.sha224(sql).hexdigest()
    key = "sql_cache:" + hash
    if options.debug:
      print "Created Key\t : %s" % key
    
    # Check if data is in cache.
    try:
      res = cPickle.loads(R_SERVER.get(key))
      if options.debug:
        print "This was return from redis"    
      
    except:
      # Do PostgreSQL query  
      cur.execute(sql)
      data = cur.fetchall()
        
      # Put data into cache for TTL time
      R_SERVER.set(key, cPickle.dumps(data) )
      R_SERVER.expire(key, TTL);
 
      if options.debug:
        print "Put data in redis and return the data"
      res=cPickle.loads(R_SERVER.get(key))

    return res

def bench():
  for i in range(0, loops):
    try:
      if options.redis:
        results = cache_redis(sql)
      else:
        cur.execute(sql)
        results = cur.fetchall()
      if options.debug:
        print results
    except:
      print "query failed!"
    # print some progress info every 1000 queries
    # so the user doesn't go to sleep
    if i % 1000 == 0:
      print i

if __name__ == "__main__":
  bench()
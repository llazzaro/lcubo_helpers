# -*- coding: utf-8 -*-
import os
import time
import json

from sqlalchemy import create_engine
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.orm.session import sessionmaker

ftime = time.time() * 1000000


def incremental_filename(dump_directory, orig_filename):
    base_path = dump_directory
    orig_filename, extension = os.path.splitext(orig_filename)
    i = 0
    filename = '{0}_{1}{2}'.format(orig_filename, i, extension)
    while os.path.exists(os.path.join(base_path, filename)):
        filename = '{0}_{1}{2}'.format(orig_filename, i, extension)
        i += 1

    return filename


def new_directory(dump_directory):
    base_path = dump_directory
    i = 0
    while os.path.isdir(os.path.join(base_path, str(i))):
        i += 1

    res = os.path.join(base_path, str(i))
    os.mkdir(res)
    return res


def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement))
        params.update(defaults or {})
        instance = model(**params)
        session.add(instance)
        return instance, True


def init_session(database_filename):
    engine = create_engine('sqlite:///{0}'.format(database_filename), echo=False)
    DBSession = scoped_session(
        sessionmaker(
            autoflush=True,
            autocommit=False,
            bind=engine
        )
    )
    return DBSession


def pushqueue_json(redis_server, key, data):
    data = json.dumps(data)
    redis_server.rpush(key, data)


def popqueue_json(redis_server, key):
    redis_server.lpop(key)


def load_json(redis_server, key):
    data = redis_server.get(key)
    if data:
        return json.loads(data.decode('utf8'))
    return {}


def save_json(redis_server, key, data):
    redis_server.set(key, json.dumps(data))


def uptime():
    microtime = int(round(time.time() * 1000000)) - ftime
    return microtime

def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):                                                                                                                                                  
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol) 


def memoize(function):
  memo = {}
  def wrapper(*args):
    if args in memo:
      return memo[args]
    else:
      rv = function(*args)
      memo[args] = rv
      return rv
  return wrapper

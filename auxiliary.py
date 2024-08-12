import os
import re
import duckdb as d
from typing import Generator
from . import cx, tn, tn_prefix, tn_sep

def table_name(name_list):
  if len(name_list)==0:
    new_name=f"{tn_prefix}{tn_sep}0"
  else:
    parts=name_list[-1].split(tn_sep)
    last_one=parts[-1]

def to_table(object):
  """
  Convert given object to internal DuckDB relation representation
  """
    
  if isinstance(object,d.duckdb.DuckDBPyRelation):
    try:
      object_name=object.__name__
    except AttributeError as e:
      object_name=generate_name()

    cx.register(f"{object_name}_tmp",object)
    cx.sql(f"CREATE OR REPLACE TABLE {object_name} AS SELECT * FROM {object_name}_tmp")
    if ('duckdb_verbose' in globals()) and duckdb_verbose:
      print(f"{object_name=}")
    return cx.sql(f"SELECT * FROM {object_name}")
  else:
    return object

def set_type(object,column,new_type):
  if isinstance(object, dict) and ('n' in object):
    d.execute(f"ALTER TABLE {object['n']} ALTER COLUMN {column} TYPE {new_type}")

def name_generator()->Generator[str, None, None]:
  """
  Generate a table name, a sequenced str
  """
  seq=0
  while True:
    yield f"{tn_prefix}{tn_sep}{seq}"
    seq+=1

tn_gen=name_generator()
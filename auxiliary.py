import os
import re
import duckdb as d
import pandas as pd
import numpy as np
from typing import Generator
from . import cx, tn, tn_prefix, tn_sep

def table_name(name_list):
  if len(name_list)==0:
    new_name=f"{tn_prefix}{tn_sep}0"
  else:
    parts=name_list[-1].split(tn_sep)
    last_one=parts[-1]

def set_type(object,column,new_type):
  if isinstance(object, d.duckdb.DuckDBPyRelation):
    # Return everything except the named column then the casted named column
    # new_rel=object.project(f"* EXCLUDE {column}, CAST({column} AS {new_type}) AS {column}")
    table_name=next(tn_gen)
    table=object.to_table(table_name)
    alter_str=f"ALTER TABLE {table_name} ALTER {column} TYPE {new_type}"
    d.sql(alter_str)
    new_rel=d.sql(f"FROM {table_name}")
    return new_rel

def name_generator()->Generator[str, None, None]:
  """
  Generate a table name, a sequenced str
  """
  seq=0
  while True:
    yield f"{tn_prefix}{tn_sep}{seq}"
    seq+=1

tn_gen=name_generator()
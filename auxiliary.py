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
    new_rel=object.project(f"* EXCLUDE {column}, CAST({column} AS {new_type}) AS {column}")
    return new_rel

def name_generator()->Generator[str, None, None]:
  """
  Generate a table name, a sequenced str
  """
  seq=0
  while True:
    yield f"{tn_prefix}{tn_sep}{seq}"
    seq+=1

def add_columns(relobject, colobject, colname=None):
  if isinstance(relobject, d.duckdb.DuckDBPyRelation) and \
    isinstance(colobject, pd.core.frame.DataFrame):
    # Generate new names for objects as tables
    new_rel=next(tn_gen)
    new_pd=next(tn_gen)
    # Register objects as tables
    d.register(new_rel, relobject)
    d.register(new_pd, colobject)
    if colname:
      return d.sql(f"SELECT {new_rel}.*, {new_pd}.* AS {colname} FROM {new_rel} POSITIONAL JOIN {new_pd}")
    else:
      return d.sql(f"SELECT {new_rel}.*, {new_pd}.* FROM {new_rel} POSITIONAL JOIN {new_pd}")
  if isinstance(relobject, d.duckdb.DuckDBPyRelation) and \
    isinstance(colobject, np.ndarray):
    if colobject.ndim<2 or (colobject.ndim==2 and ((colobject.shape[0]==1) or (colobject.shape[1]==1))):
      return d.sql(f"SELECT relobject.*, colobject.* AS {colname} FROM relobject POSITIONAL JOIN colobject")
    else:
      return d.sql(f"SELECT relobject.*, colobject.* FROM relobject POSITIONAL JOIN colobject")
    
tn_gen=name_generator()
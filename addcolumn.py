import duckdb as d
import pandas as pd
import numpy as np
# import polars as pl
# from typing import Generator
from . import cx, tn, tn_prefix, tn_sep, tn_gen

def add_column(relobject, colobject, colname=None):
  """
  Add colobject as a new table column to relobject, using colname as the name of the new
  column
  """
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

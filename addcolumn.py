import duckdb as d
import pandas as pd
import numpy as np
from . import cx, tn, tn_prefix, tn_sep, tn_gen

def add_column(object, columndata, newcolname=None):
  """
  Add columndata as a new table column to object, using newcolname as the name of the new
  column
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) and \
    isinstance(columndata, pd.core.frame.DataFrame):
    # Generate new names for objects as tables
    new_rel=next(tn_gen)
    new_pd=next(tn_gen)
    # Register objects as tables
    d.register(new_rel, object)
    d.register(new_pd, columndata)
    if newcolname:
      return d.sql(f"SELECT {new_rel}.*, {new_pd}.* AS {newcolname} FROM {new_rel} POSITIONAL JOIN {new_pd}")
    else:
      return d.sql(f"SELECT {new_rel}.*, {new_pd}.* FROM {new_rel} POSITIONAL JOIN {new_pd}")
  if isinstance(object, d.duckdb.DuckDBPyRelation) and \
    isinstance(columndata, np.ndarray):
    if columndata.ndim<2 or (columndata.ndim==2 and ((columndata.shape[0]==1) or (columndata.shape[1]==1))):
      return d.sql(f"SELECT object.*, columndata.* AS {newcolname} FROM object POSITIONAL JOIN columndata")
    else:
      return d.sql(f"SELECT object.*, columndata.* FROM object POSITIONAL JOIN columndata")

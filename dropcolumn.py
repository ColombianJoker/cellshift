import duckdb as d
# import pandas as pd
# import polars as pl
from . import cx

def drop_column(object, column):
  """
  Returns the object without a column
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(column,str):
    # Return all columns except the named one
    return object.project(f"* EXCLUDE {column}")
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(column,list):
      columns=", ".join(str(item) for item in column)
      return object.project(f"* EXCLUDE ({columns})")
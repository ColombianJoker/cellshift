import duckdb as d
# import pandas as pd
# import polars as pl
from . import cx

def drop_column(object, column):
  """
  Returns the object without a column
  """
  if isinstance(object, dict) and ('n' in object):
    d.sql(f"ALTER TABLE {object['n']} DROP {column}")
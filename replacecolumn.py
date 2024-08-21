import duckdb as d
# import pandas as pd
# import polars as pl
# from typing import Generator
from . import cx, tn, tn_prefix, tn_sep, tn_gen

def replace_column(relobject, coltoreplace, replacingcol):
  """
  replace the complete contents of coltoreplace column in relobject with the contents of
  replacingcol
  """
  if isinstance(relobject, d.duckdb.DuckDBPyRelation) and \
    isinstance(coltoreplace,str) and \
    isinstance(replacingcol,str):
    new_name=next(tn_gen)
    relobject.to_table(new_name)
    alter_str=f"UPDATE {new_name} SET {coltoreplace}={replacingcol}"
    d.sql(alter_str)
    relobject=d.sql(f"SELECT * FROM {new_name}")
  return relobject
   
# tn_gen=name_generator()
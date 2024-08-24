import duckdb as d
from duckdb.typing import *
# from math import ceil, floor
# import pandas as pd
# import polars as pl
# from typing import Generator
from . import cx, tn, tn_prefix, tn_sep, tn_gen
from . import add_column, replace_column, drop_column, set_type
# from IPython.display import display

def mask_val(value, maskleft=0, maskright=0, maskchar="0"):
  """
  Return a string equivalent to the given value with the maskleft left digits, and the
  maskright right digits replaced by maskchar
  """
  if len(maskchar)>0:
    maskchar=maskchar[0]
    masked=str(value)
    if masked[0]=="-":
      sign=-1
      masked=masked[1:]
    else:
      sign=1
    if (len(masked)>maskleft) and (len(masked)>maskright):
      if maskleft>0:
        # masked=masked[maskleft:]
        masked=f"{maskchar}"*maskleft+masked[maskleft:]
      if maskright>0:
        # masked=masked[:-maskright]
        masked=masked[:-maskright]+f"{maskchar}"*maskright
      if sign<0:
        masked="-"+masked
    else:
      masked=str(value) if sign>0 else "-"+str(value)
    return masked
  else:
    return value

def add_masked_column(object, basecolumn, new_colname, maskleft=0, maskright=0, maskchar="×"):
  """
  Add a string column of masked values to object, with the maskleft leftmost characters replaced
  by the mask, and the maskright rightmost characters replaced by the mask. The funtion only
  uses the first char in maskchar.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str) \
    and isinstance(new_colname,str):
    d.create_function('mask_val', mask_val, [VARCHAR, INTEGER, INTEGER, VARCHAR], VARCHAR)
    table_name=next(tn_gen)
    table=object.to_table(table_name)
    add_col_str=f"ALTER TABLE {table_name} ADD COLUMN {new_colname} VARCHAR"
    d.sql(add_col_str)
    table=d.sql(f"FROM {table_name}")
    # display(table.limit(10))
    upd_col_str=f"UPDATE {table_name} SET {new_colname}=mask_val({basecolumn},{maskleft},{maskright},'{maskchar}')"
    # print(f"{upd_col_str=}")
    d.sql(upd_col_str)
    table=d.sql(f"FROM {table_name}")
    return table
  return object

def masked_column(object, column, maskleft=0, maskright=0, maskchar="•"):
  """
  Replace all values of column with masked string values.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(column,str):
    base_column=f"{column}"
    new_colname=f"{column}_masked"
    table=add_masked_column(object,base_column,new_colname, maskleft=maskleft, maskright=maskright, maskchar=maskchar)
    table=replace_column(table,base_column,new_colname)
    table=drop_column(table,new_colname)
    return table
  return object
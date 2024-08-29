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

def add_masked_column_bigint(object, basecolumn, new_colname, maskleft=0, maskright=0, maskchar="×"):
  """
  Add a string column of masked values to object, with the maskleft leftmost characters of 
  INTEGER basecolumn replaced by the mask, and the maskright rightmost characters replaced by the
  mask. The funtion only uses the first char in maskchar.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str) \
    and isinstance(new_colname,str):
    table_name=next(tn_gen)
    table=object.to_table(table_name)
    # Check if the column is of INTEGER type
    type_count_str=f"SELECT COUNT(*) FROM duckdb_columns() WHERE table_name='{table_name}' AND "
    type_count_str+=f"(column_name='{basecolumn}' AND data_type LIKE '%INT%')"
    int_type_count=d.sql(type_count_str).fetchall()[0][0]
    if int_type_count>0:
      # Column is of INTEGER type
      # Check if the INTEGER function is registered
      fn_count_str=f"SELECT COUNT(*) FROM duckdb_functions() WHERE function_name='mask_BIGINT_val'"
      int_fn_count=d.sql(fn_count_str).fetchall()[0][0]
      if int_fn_count==0:
        # Function is not registered, register it
        d.create_function('mask_BIGINT_val', mask_val, [BIGINT, INTEGER, INTEGER, VARCHAR], VARCHAR)
        # end register
      add_col_str=f"ALTER TABLE {table_name} ADD COLUMN {new_colname} VARCHAR"
      d.sql(add_col_str)
      table=d.sql(f"FROM {table_name}")
      upd_col_str=f"UPDATE {table_name} SET {new_colname}=mask_BIGINT_val({basecolumn},{maskleft},{maskright},'{maskchar}')"
      d.sql(upd_col_str)
      table=d.sql(f"FROM {table_name}")
      return table
      # end INTEGER
    return table
  return object

def add_masked_column_varchar(object, basecolumn, new_colname, maskleft=0, maskright=0, maskchar="×"):
  """
  Add a string column of masked values to object, with the maskleft leftmost characters of 
  VARCHAR basecolumn replaced by the mask, and the maskright rightmost characters replaced by the
  mask. The funtion only uses the first char in maskchar.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str) \
    and isinstance(new_colname,str):
    table_name=next(tn_gen)
    table=object.to_table(table_name)
    # Check if the column is of VARCHAR type
    type_count_str=f"SELECT COUNT(*) FROM duckdb_columns() WHERE table_name='{table_name}' AND "
    type_count_str+=f"(column_name='{basecolumn}' AND data_type LIKE 'VARCHAR%')"
    varchar_type_count=d.sql(type_count_str).fetchall()[0][0]
    if varchar_type_count>0:
      # Column is of VARCHAR type
      # Check if the VARCHAR function is registered
      fn_count_str=f"SELECT COUNT(*) FROM duckdb_functions() WHERE function_name='mask_VARCHAR_val'"
      varchar_fn_count=d.sql(fn_count_str).fetchall()[0][0]
      if varchar_fn_count==0:
        # Function is not registered, register it
        d.create_function('mask_VARCHAR_val', mask_val, [VARCHAR, INTEGER, INTEGER, VARCHAR], VARCHAR)
        # end register
      add_col_str=f"ALTER TABLE {table_name} ADD COLUMN {new_colname} VARCHAR"
      d.sql(add_col_str)
      table=d.sql(f"FROM {table_name}")
      upd_col_str=f"UPDATE {table_name} SET {new_colname}=mask_VARCHAR_val({basecolumn},{maskleft},{maskright},'{maskchar}')"
      d.sql(upd_col_str)
      table=d.sql(f"FROM {table_name}")
      return table
      # end VARCHAR
    return table
  return object

def add_masked_column(object, basecolumn, new_colname, maskleft=0, maskright=0, maskchar="×"):
  """
  Add a string column of masked values to object, with the maskleft leftmost characters replaced
  by the mask, and the maskright rightmost characters replaced by the mask. The funtion only
  uses the first char in maskchar.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str) \
    and isinstance(new_colname,str):
    table_name=next(tn_gen)
    table=object.to_table(table_name)
    # Check type of column
    # Case BIGINT
    type_count_str=f"SELECT COUNT(*) FROM duckdb_columns() WHERE table_name='{table_name}' AND "
    type_count_str+=f"(column_name='{basecolumn}' AND data_type LIKE '%INT%')"
    bigint_type_count=d.sql(type_count_str).fetchall()[0][0]
    if bigint_type_count>0:
      return add_masked_column_bigint(object, basecolumn, new_colname, maskleft, maskright, maskchar)
    # Case VARCHAR
    type_count_str=f"SELECT COUNT(*) FROM duckdb_columns() WHERE table_name='{table_name}' AND "
    type_count_str+=f"(column_name='{basecolumn}' AND data_type LIKE 'VARCHAR%')"
    varchar_type_count=d.sql(type_count_str).fetchall()[0][0]
    if varchar_type_count>0:
      return add_masked_column_varchar(object, basecolumn, new_colname, maskleft, maskright, maskchar)
    return table
  return object

def masked_column(object, basecolumn, maskleft, maskright, maskchar):
  """
  Replace all values of selected column with VARCHAR versions of the values with leftmost maskleft
  characters replaced with maskchar and rightmost maskright characters replaced with maskchar.
  The function only uses the first char in maskchar
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str):
    base_column=f"{basecolumn}"
    new_colname=f"{basecolumn}_masked"
    table_name=next(tn_gen)
    table=object.to_table(table_name)
    table=add_masked_column(object,basecolumn,new_colname, maskleft, maskright, maskchar)
    # Check type of column
    # Case BIGINT
    type_count_str=f"SELECT COUNT(*) FROM duckdb_columns() WHERE table_name='{table_name}' AND "
    type_count_str+=f"(column_name='{basecolumn}' AND data_type LIKE '%INT%')"
    bigint_type_count=d.sql(type_count_str).fetchall()[0][0]
    if bigint_type_count>0:
      table=drop_column(table,basecolumn)
      table_name_bigint=next(tn_gen)
      table=table.to_table(table_name_bigint)
      # table=d.sql(f"FROM {table_name_bigint}")
      alter_table_str=f"ALTER TABLE {table_name_bigint} RENAME {new_colname} TO {basecolumn}"
      d.sql(alter_table_str)
      table=d.sql(f"FROM {table_name_bigint}")
      return table
    # Case VARCHAR
    type_count_str=f"SELECT COUNT(*) FROM duckdb_columns() WHERE table_name='{table_name}' AND "
    type_count_str+=f"(column_name='{basecolumn}' AND data_type LIKE 'VARCHAR%')"
    varchar_type_count=d.sql(type_count_str).fetchall()[0][0]
    if varchar_type_count>0:
      table=replace_column(table,basecolumn,new_colname)
      table=drop_column(table,new_colname)
      return table
  return object
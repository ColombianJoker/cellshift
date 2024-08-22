import duckdb as d
from math import ceil
# import pandas as pd
# import polars as pl
# from typing import Generator
from . import cx, tn, tn_prefix, tn_sep, tn_gen

def add_integer_range_column(object, basecolumn, new_colname, minvalue=0, maxvalue=None, numranges=None, rangesize=10, onlystart=False):
  """
  Add number range column to object, basing its values from a preexisting one, base column must be
  numeric.
  Each range starts from minvalue and goes to minvalue + n×rangesize-1
  if maxvalue and numranges is given, they are used to calculate the start and end of the ranges
  if onlystart==True then the column is scalar and only will contain the start of each range
  if onlystart==False then the column is a list and will contain the start and end of each range
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str) \
    and isinstance(new_colname,str):
    table_name=next(tn_gen)
    table=object.to_table(table_name)
    if onlystart:
      add_col_str=f"ALTER TABLE {table_name} ADD COLUMN {new_colname} INTEGER"
    else:
      add_col_str=f"ALTER TABLE {table_name} ADD COLUMN {new_colname} INTEGER[]"
    # print(f"{add_col_str=}")
    d.sql(add_col_str)
    if numranges:
      # Create the ranges
      if maxvalue:
        total_range=maxvalue-minvalue
      else:
        max_val_str=f"SELECT MAX({basecolumn}) FROM object"
        max_val_tuple=d.sql(max_val_str).fetchone()
        maxvalue=max_val_tuple[0]
      rangesize=ceil(maxvalue-minvalue)/numranges
      if onlystart:
        upd_col_str=f"UPDATE {table_name} SET {new_colname}={minvalue}+{rangesize}*(FLOOR(({basecolumn}-{minvalue})/{rangesize}))"
      else:
        rstart_str=f"{minvalue}+{rangesize}*(FLOOR(({basecolumn}-{minvalue})/{rangesize}))"
        rend_str=f"{minvalue}+{rangesize}*(FLOOR(({basecolumn}-{minvalue})/{rangesize}))+{rangesize}-1"
        upd_col_str=f"UPDATE {table_name} SET {new_colname}=[{rstart_str},{rend_str}]"
    else:
      # Ranges are given
      if onlystart:
        upd_col_str=f"UPDATE {table_name} SET {new_colname}={minvalue}+{rangesize}*(FLOOR(({basecolumn}-{minvalue})/{rangesize}))"
      else:
        rstart_str=f"{minvalue}+{rangesize}*(FLOOR(({basecolumn}-{minvalue})/{rangesize}))"
        rend_str=f"{minvalue}+{rangesize}*(FLOOR(({basecolumn}-{minvalue})/{rangesize}))+{rangesize}-1"
        upd_col_str=f"UPDATE {table_name} SET {new_colname}=[{rstart_str},{rend_str}]"
    # print(f"{upd_col_str=}")
    d.sql(upd_col_str)
    table=d.sql(f"FROM {table_name}")
    return table
  return object

def add_age_range_column(object, basecolumn, new_colname, minage=0, maxage=None, numranges=None, agerange=10, onlystart=False, onlyadults=True, adultage=18):
  """
  Add age range column to object, basing its values from a preexisting one, base column must be numeric
  Each range starts from minvalue and goes to minvalue + n×rangesize-1
  if maxvalue and numranges is given, they are used to calculate the start and end of the ranges
  if onlystart==True then the column is scalar and only will contain the start of each range
  if onlystart==False then the column is a list and will contain the start and end of each range
  if onlyadults==True then will filter and return only the records with basecolumn>=adultage
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
  and isinstance(basecolumn,str) \
  and isinstance(new_colname,str):
    table=add_integer_range_column(object,basecolumn,new_colname,minvalue=minage,maxvalue=maxage,numranges=numranges,rangesize=agerange,onlystart=onlystart)
    if onlyadults:
      table=table.filter(f"{basecolumn}>={adultage}")
    return table
  return object

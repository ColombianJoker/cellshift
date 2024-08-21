import duckdb as d
import numpy as np
# import pandas as pd
# import polars as pl
from . import cx, tn, tn_prefix, tn_sep, tn_gen
from . import add_column, replace_column, drop_column

def add_gaussian_noise_column(object, basecolumn, newcolname=None):
  """
  Add gaussian noise column to object
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str) \
    and isinstance(newcolname,str):
    stat_count,stat_mean,stat_dev=d.execute(f"SELECT COUNT({basecolumn}), AVG({basecolumn}), STDDEV_POP({basecolumn}) FROM object").fetchone()
    gaussian_noise=np.random.normal(stat_mean, stat_dev, (stat_count,1))
    if newcolname:
      return add_column(object,np.transpose(gaussian_noise),newcolname)
    else:
      return add_column(object,np.transpose(gaussian_noise))
    
def gaussian_column(object, column):
  """
  Replace all values in column of object with noised values
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(column,str):
    base_column=f"{column}"
    new_colname=f"noised_{column}"
    # print(f"{type(object)=} <- noise_column()")
    table=add_gaussian_noise_column(object,column,new_colname)
    # register the table is only needed to debug average and std deviation
    # table_name=next(tn_gen)
    # d.register(table_name,table)
    # print(f"{type(table)=} <- add_gaussian_noise_column()")
    # base_stats=d.sql(f"SELECT COUNT({base_column}), AVG({base_column}), STDDEV_POP({base_column}) FROM {table_name}").fetchone()
    # noised_stats=d.sql(f"SELECT COUNT({new_colname}), AVG({new_colname}), STDDEV_POP({new_colname}) FROM {table_name}").fetchone()
    # print(f"base: count={base_stats[0]}, avg={base_stats[1]}, dev={base_stats[2]}")
    # print(f"base: count={noised_stats[0]}, avg={noised_stats[1]}, dev={noised_stats[2]}")
    table=replace_column(table,column,new_colname)
    # print(f"{type(table)=} <- replace_column()")
    table=drop_column(table,new_colname)
    # print(f"{type(table)=} <- drop_column()")
    return table
  return object
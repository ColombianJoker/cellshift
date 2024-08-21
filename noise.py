import duckdb as d
import numpy as np
# import pandas as pd
# import polars as pl
from IPython.display import display
from tqdm import tqdm
from . import cx, tn, tn_prefix, tn_sep, tn_gen
from . import add_column, replace_column, drop_column


def add_gaussian_noise_column(object, basecolumn, newcolname=None):
  """
  Add gaussian noise column to object, base column must be numeric, and newcolname is always
      of float type
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

def add_impulse_noise_column(object, basecolumn, newcolname, samplepct=None, nsamples=None, impulsemag=None, impulsepct=None, verbose=False):
  """
  Add impulse noise column to object, base column must be numeric, and newcolname is always
      of float type.
      samplepct: percent of column values to be altered by noise
      nsamples:  absolute number of column values to be alterded by noise
      impulsemag: max of impulse noise to be applied to some column values
      impulsepct: percent of max value in column to be applied to some column values
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str) \
    and isinstance(newcolname,str):
    if samplepct or nsamples:
      # Continue
      if impulsemag or impulsepct:
        # Continue
        # print(f"{samplepct=}, {nsamples=}, {impulsemag=}, {impulsepct=}")
        if impulsemag:
          # Si absolute impulse noise was given, use that value
          max_impulse=impulsemag
        else:
          max_str=f"SELECT MAX(ABS({basecolumn})) FROM object"
          max_v_tuple=d.sql(max_str).fetchone()
          max_impulse=(max_v_tuple[0]*impulsepct)/100
          # print(f"{impulsepct=}, {max_impulse=}")
        count=0
        if not nsamples:
          count_str=f"SELECT COUNT(*) FROM object"
          count_tuple=d.sql(count_str).fetchone()
          count=count_tuple[0]
          nsamples=max(1,int((samplepct*count)/100))
          # print(f"{count=}, {nsamples=}")
        random_rows=np.random.randint(0,count,nsamples)
        table_name=next(tn_gen)
        object.to_table(table_name)
        add_col_str=f"ALTER TABLE {table_name} ADD COLUMN {newcolname} FLOAT"
        d.sql(add_col_str)
        table=d.sql(f"FROM {table_name}")
        upd_col_str=f"UPDATE {table_name} SET {newcolname}={basecolumn}"
        d.sql(upd_col_str)
        table=d.sql(f"FROM {table_name}")
        for row_id in tqdm(random_rows, disable=not verbose):
          # print(f"{row_id=}")
          noise=np.random.uniform(-max_impulse,max_impulse)
          # print(f"{noise=}")
          update_str=f"UPDATE {table_name} SET {newcolname}={basecolumn}+{noise} WHERE rowid=={row_id}"
          d.sql(update_str)
        table=d.sql(f"FROM {table_name}")
        return table
      else:
        # if impulsemag and nsamples were not given
        return object
    else:
      # if samplepct and nsamples were not given
      return object
  return object
    
def add_salt_pepper_noise_column(object, basecolumn, newcolname, samplepct=None, nsamples=None, verbose=False):
  """
  Add salt & pepper noise column to object, base column must be numeric, and newcolname is always
      of float type.
      samplepct: percent of column values to be altered by noise
      nsamples:  absolute number of column values to be alterded by noise
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn,str) \
    and isinstance(newcolname,str):
    if samplepct or nsamples:
      # Continue
      max_str=f"SELECT MAX({basecolumn}), MIN({basecolumn}) FROM object"
      max_v_tuple=d.sql(max_str).fetchone()
      max_noise=max_v_tuple[0]
      min_noise=max_v_tuple[1]
      count=0
      # print(f"{min_noise=}, {max_noise=}")
      if not nsamples:
        count_str=f"SELECT COUNT(*) FROM object"
        count_tuple=d.sql(count_str).fetchone()
        count=count_tuple[0]
        nsamples=max(1,int((samplepct*count)/100))
      # print(f"{count=}, {nsamples=}")
      random_rows=np.random.randint(0,count,nsamples)
      # print(f"{random_rows[:50]=}")
      table_name=next(tn_gen)
      object.to_table(table_name)
      add_col_str=f"ALTER TABLE {table_name} ADD COLUMN {newcolname} FLOAT"
      # print(f"{add_col_str=}")
      d.sql(add_col_str)
      table=d.sql(f"FROM {table_name}")
      upd_col_str=f"UPDATE {table_name} SET {newcolname}={basecolumn}"
      d.sql(upd_col_str)
      table=d.sql(f"FROM {table_name}")
      for row_id in tqdm(random_rows, disable=not verbose):
        noise=np.random.choice([min_noise,max_noise]) # Choose or min_noise or max_noise
        update_str=f"UPDATE {table_name} SET {newcolname}={noise} WHERE rowid=={row_id}"
        d.sql(update_str)
      table=d.sql(f"FROM {table_name}")
      return table
    else:
      # if samplepct and nsamples were not given
      return object
  return object

def gaussian_column(object, column):
  """
  Replace all values in column of object with noised values using gausian distribution noise.
  DuckDB will round integer column with integer values of float gaussian noise, then average
  and std deviation will suffer a light variation (first decimal place for average and second
  decimal place for std dev)
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(column,str):
    base_column=f"{column}"
    new_colname=f"noised_{column}"
    table=add_gaussian_noise_column(object,column,new_colname)
    table=replace_column(table,column,new_colname)
    table=drop_column(table,new_colname)
    return table
  return object

def impulse_column(object, column, samplepct=None, nsamples=None, impulsemag=None, impulsepct=None, verbose=False):
  """
  Replace some values in column of object with noised values, using impulse noise values, using
  uniform randomized choices. DuckDB will round integer column with integer values of float
  impulse noise. There is no way to maintain the average and standard deviation of whole column
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(column,str):
    base_column=f"{column}"
    new_colname=f"noised_{column}"
    # display(d.sql(f"SELECT AVG({column}) AS AVG_{column}, STDDEV_POP({column}) AS DEV FROM object"))
    table=add_impulse_noise_column(object,column,new_colname,impulsepct=impulsepct,samplepct=samplepct,verbose=verbose)
    # new_name=next(tn_gen)
    # d.register(new_name,table)
    # display(d.sql(f"SELECT AVG({new_colname}) AS AVG_{new_colname}, STDDEV_POP({new_colname}) AS DEV FROM {new_name}"))
    table=replace_column(table,column,new_colname)
    table=drop_column(table,new_colname)
    return table
  return object

def salt_pepper_column(object, column, samplepct=None, nsamples=None, verbose=False):
  """
  Replace some values in column of object with noised values, using salt & pepper noise values,
  using uniform randomized between the max and the min. DuckDB will round integer column with integer
  values of float impulse noise. There is no way to maintain the average and standard deviation of
  whole column
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(column,str):
    base_column=f"{column}"
    new_colname=f"noised_{column}"
    # display(d.sql(f"SELECT AVG({column}) AS AVG_{column}, STDDEV_POP({column}) AS DEV FROM object"))
    table=add_salt_pepper_noise_column(object,column,new_colname,samplepct=samplepct,verbose=verbose)
    # new_name=next(tn_gen)
    # d.register(new_name,table)
    # display(d.sql(f"SELECT AVG({new_colname}) AS AVG_{new_colname}, STDDEV_POP({new_colname}) AS DEV FROM {new_name}"))
    table=replace_column(table,column,new_colname)
    table=drop_column(table,new_colname)
    return table
  return object
import duckdb as d
import numpy as np
# import pandas as pd
# import polars as pl
from . import cx
from . import add_column

def add_gaussian(object, column, colname=None):
  """
  Add gaussian noise to numeric column
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation):
    stat_count,stat_mean,stat_dev=d.execute(f"SELECT COUNT({column}), AVG({column}), STDDEV_POP({column}) FROM object").fetchone()
    gaussian_noise=np.random.normal(stat_mean, stat_dev, (stat_count,1))
    if colname:
      return add_column(object,np.transpose(gaussian_noise),colname)
    else:
      return add_column(object,np.transpose(gaussian_noise))
    
  
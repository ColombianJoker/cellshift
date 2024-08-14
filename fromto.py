import os
import duckdb as d
import pandas as pd
import polars as pl

def to_duckdb(object,readopts=None):
  # If the object is a string is considered a filename
  if isinstance(object, str):
    # This is a file name
    object_filename, object_fileext = os.path.splitext(object)
    try:
      match (object_fileext.lower()):
        case ".json":
          new_rel=d.read_json(object)            ## read JSON and return
        case ".parquet":
          new_rel=d.read_parquet(object)         ## read PARQUET and return
        case _:                                  ## default is CSV        
          new_rel=d.read_csv(object)
    except:
      pass
    return new_rel

  # If the object is a DuckDB relation, return it the same
  if isinstance(object,d.duckdb.DuckDBPyRelation):
    return object                             ## return it

  # If the object is a Pandas DataFrame, convert it and return it
  if isinstance(object,pd.core.frame.DataFrame):
    new_rel=d.sql(f"SELECT * FROM object")
    return new_rel
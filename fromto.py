import os
import duckdb as d
import pandas as pd
import polars as pl
from . import cx, tn
from .auxiliary import tn_gen

def to_duckdb(object,readopts=None):
  # If the object is a string is considered a filename
  new_name=next(tn_gen)
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
          if readopts:
            d.sql(f"                             \
            CREATE TABLE {new_name} AS           \
            SELECT * FROM read_csv('{object}')   \
            ")
          else:
            d.sql(f"                             \
            CREATE TABLE {new_name} AS           \
            SELECT * FROM read_csv('{object}')   \
            ")
    except:
      pass
    new_rel=d.sql(f"SELECT * FROM {new_name}")
    tn.append(new_name)
    return {"t":new_rel, "n":new_name}

  # If the object is a DuckDB relation, return it the same
  if isinstance(object,d.duckdb.DuckDBPyRelation):
    return object                             ## return it

  # If the object is a Pandas DataFrame, convert it and return it
  if isinstance(object,pd.core.frame.DataFrame):
    d.sql(f"CREATE TABLE {new_name} AS SELECT * from object")
    new_rel=d.sql(f"SELECT * FROM {new_name}")
    tn.append(new_name)
    return {"t":new_rel, "n":new_name}
## INIT ##
# Tool to transform, obfuscate and anonymize datasets
# Ramón Barrios Láscar
# 2024-08-09

# Import popular modules
import duckdb as d
import pandas as pd
import polars as pl

tn_prefix="CSTABLE"
tn_sep="_"
tn=[] # Table name list

if 'duckdb_config' in globals():
  cx=d.connect(
    config=duckdb_config
  )
  if ('duckdb_verbose' in globals()) and duckdb_verbose:
    print(f"DuckDB connected to default", file=sys.stderr)
else:
  cx=d.connect()
  if ('duckdb_verbose' in globals()) and duckdb_verbose:
    print(f"DuckDB connected to default", file=sys.stderr)

# Import modules
from .fromto import *
from .auxiliary import *
from .dropcolumn import *
from .addcolumn import *
from .replacecolumn import *
from .noise import *
from .ranges import *
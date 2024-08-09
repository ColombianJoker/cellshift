## INIT ##
# Tool to transform, obfuscate and anonymize datasets
# Ramón Barrios Láscar
# 2024-08-09

import duckdb as d
import pandas as pd
import polars as pl

if 'duckdb_verbose' in globals():
  import sys

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
  
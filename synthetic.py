import duckdb as d
# from duckdb.typing import *
from . import cx, tn, tn_prefix, tn_sep, tn_gen, random_code, letters_for
from . import add_column, replace_column, drop_column, set_type
from faker import Faker
from datetime import datetime
from tqdm import tqdm

def add_syn_date_column(object, basecolumn=None, newcolname=None, startdate=None, enddate=None, dateformat="%Y-%m-%d", verbose=False):
  """
  Add a newcolname date column to object using dateformat format. If basecolumn and startdate are
  given but not enddate, it creates the dates between basecolumn and startdate.
  If basecolumn and enddate are given, it creates dates between basecolumn and end date. If 
  startdate and enddate are given, it creates dates between these.
  It searches for global fake and locale objects. It always creates the same number of fake dates
  than the number of records on the base object table.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(newcolname, str):
    random_date=False # Only to check
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    if startdate and isinstance(startdate, str):
      startdate=datetime.strptime(startdate, dateformat)
    if enddate and isinstance(enddate, str):
      enddate=datetime.strptime(enddate, dateformat)
    table_name=next(tn_gen)
    table=object.to_table(table_name)
    how_many=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
    d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} DATE")
    for row_id in tqdm(range(how_many), disable=not verbose):
      if startdate and enddate:
        random_date=fake.date_between(start_date=startdate, end_date=enddate)
      else:
        if basecolumn:
          base_date=d.sql(f"SELECT {basecolumn} FROM {table_name} WHERE rowid=={row_id}").fetchall()[0][0]
        if startdate:
          random_date=fake.date_between(start_date=startdate, end_date=base_date)
        if enddate:
          random_date=fake.date_between(start_date=base_date, end_date=enddate)
      if random_date:
        d.sql(f"UPDATE {table_name} SET {newcolname}='{random_date}' WHERE rowid={row_id}")
      else:
        print(f"Not enough parameters to create fake dates!")
        break
    return d.sql(f"FROM {table_name}")
  return object

def add_syn_city_column(object, basecolumn=None, newcolname=None, maxuniques=1000, verbose=False):
  """
  Add a newcolname VARCHAR column with town/city names. If basecolum is valid (is a column of the
  object) and the unique number of names in it are max maxunique. If the number of unique is greater
  than maxunique, it will only generate names without trying to maintain an equivalence.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(newcolname, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    if isinstance(basecolumn,str):
      # Get unique names in basecolumn
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      n_uniques=d.sql(f"SELECT COUNT(DISTINCT {basecolumn}) FROM {table_name}").fetchall()[0][0]
      if n_uniques>maxuniques:
        # Generate without equivalences
        d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
        all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
        for row_id in tqdm(range(all_count), disable=not verbose):
          fake_city=fake.city()
          d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_city}' WHERE rowid=={row_id}")
      else:
        # Generate with equivalences
        # Create table of equivalences
        equiv_table=next(tn_gen)
        d.sql(f"CREATE TABLE {equiv_table} ({basecolumn} VARCHAR, {newcolname} VARCHAR UNIQUE)")
        d.sql(f"INSERT INTO {equiv_table} ({basecolumn}) SELECT DISTINCT {basecolumn} FROM {table_name}")
        with tqdm(total=n_uniques, ) as progress:
          row_id=0
          while row_id<n_uniques:
            fake_city=fake.city()
            try:
              d.sql(f"UPDATE {equiv_table} SET {newcolname}='{fake_city}' WHERE rowid=={row_id}")
              row_id+=1
              progress.update(1)
            except d.duckdb.Error as e:
              pass
        return d.sql(f"SELECT COLUMNS({table_name}.*), {equiv_table}.{newcolname} AS {newcolname} FROM {table_name}, {equiv_table} WHERE {table_name}.{basecolumn}=={equiv_table}.{basecolumn}")
      table=d.sql(f"FROM {table_name}")
      return table
    else:
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
      all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
      for row_id in tqdm(range(all_count), disable=not verbose):
        fake_city=fake.city()
        d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_city}' WHERE rowid=={row_id}")
      table=d.sql(f"FROM {table_name}")
      return table
    return object
  return object

def add_syn_name_column(object, basecolumn=None, newcolname=None, maxuniques=1000, verbose=False):
  """
  Add a newcolname VARCHAR column with people full names. If basecolum is valid (is a column of the
  object) and the unique number of names in it are max maxunique. If the number of unique is greater
  than maxunique, it will only generate names without trying to maintain an equivalence.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(newcolname, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    if isinstance(basecolumn,str):
      # Get unique names in basecolumn
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      n_uniques=d.sql(f"SELECT COUNT(DISTINCT {basecolumn}) FROM {table_name}").fetchall()[0][0]
      if n_uniques>maxuniques:
        # Generate without equivalences
        d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
        all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
        for row_id in tqdm(range(all_count), disable=not verbose):
          fake_name=fake.name()
          d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_name}' WHERE rowid=={row_id}")
      else:
        # Generate with equivalences
        # Create table of equivalences
        equiv_table=next(tn_gen)
        d.sql(f"CREATE TABLE {equiv_table} ({basecolumn} VARCHAR, {newcolname} VARCHAR UNIQUE)")
        d.sql(f"INSERT INTO {equiv_table} ({basecolumn}) SELECT DISTINCT {basecolumn} FROM {table_name}")
        with tqdm(total=n_uniques, ) as progress:
          row_id=0
          while row_id<n_uniques:
            fake_name=fake.name()
            try:
              d.sql(f"UPDATE {equiv_table} SET {newcolname}='{fake_name}' WHERE rowid=={row_id}")
              row_id+=1
              progress.update(1)
            except d.duckdb.Error as e:
              pass
        return d.sql(f"SELECT COLUMNS({table_name}.*), {equiv_table}.{newcolname} AS {newcolname} FROM {table_name}, {equiv_table} WHERE {table_name}.{basecolumn}=={equiv_table}.{basecolumn}")
      table=d.sql(f"FROM {table_name}")
      return table
    else:
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
      all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
      for row_id in tqdm(range(all_count), disable=not verbose):
        fake_name=fake.name()
        d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_name}' WHERE rowid=={row_id}")
      table=d.sql(f"FROM {table_name}")
      return table
    return object
  return object

def add_syn_last_name_column(object, basecolumn=None, newcolname=None, maxuniques=1000, verbose=False):
  """
  Add a newcolname VARCHAR column with people last names. If basecolum is valid (is a column of the
  object) and the unique number of last names in it are max maxunique. If the number of unique is 
  greater than maxunique, it will only generate last names without trying to maintain an equivalence.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(newcolname, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    if isinstance(basecolumn,str):
      # Get unique names in basecolumn
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      n_uniques=d.sql(f"SELECT COUNT(DISTINCT {basecolumn}) FROM {table_name}").fetchall()[0][0]
      if n_uniques>maxuniques:
        # Generate without equivalences
        d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
        all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
        for row_id in tqdm(range(all_count), disable=not verbose):
          fake_last_name=fake.last_name()
          d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_last_name}' WHERE rowid=={row_id}")
      else:
        # Generate with equivalences
        # Create table of equivalences
        equiv_table=next(tn_gen)
        d.sql(f"CREATE TABLE {equiv_table} ({basecolumn} VARCHAR, {newcolname} VARCHAR UNIQUE)")
        d.sql(f"INSERT INTO {equiv_table} ({basecolumn}) SELECT DISTINCT {basecolumn} FROM {table_name}")
        with tqdm(total=n_uniques, ) as progress:
          row_id=0
          while row_id<n_uniques:
            fake_name=fake.last_name()
            try:
              d.sql(f"UPDATE {equiv_table} SET {newcolname}='{fake_last_name}' WHERE rowid=={row_id}")
              row_id+=1
              progress.update(1)
            except d.duckdb.Error as e:
              pass
        return d.sql(f"SELECT COLUMNS({table_name}.*), {equiv_table}.{newcolname} AS {newcolname} FROM {table_name}, {equiv_table} WHERE {table_name}.{basecolumn}=={equiv_table}.{basecolumn}")
      table=d.sql(f"FROM {table_name}")
      return table
    else:
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
      all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
      for row_id in tqdm(range(all_count), disable=not verbose):
        fake_name=fake.last_name()
        d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_last_name}' WHERE rowid=={row_id}")
      table=d.sql(f"FROM {table_name}")
      return table
    return object
  return object

def add_syn_first_name_column(object, basecolumn=None, newcolname=None, maxuniques=1000, verbose=False):
  """
  Add a newcolname VARCHAR column with people last names. If basecolum is valid (is a column of the
  object) and the unique number of first names in it are max maxunique. If the number of unique is
  greater than maxunique, it will only generate last names without trying to maintain an equivalence.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(newcolname, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    if isinstance(basecolumn,str):
      # Get unique names in basecolumn
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      n_uniques=d.sql(f"SELECT COUNT(DISTINCT {basecolumn}) FROM {table_name}").fetchall()[0][0]
      if n_uniques>maxuniques:
        # Generate without equivalences
        d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
        all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
        for row_id in tqdm(range(all_count), disable=not verbose):
          fake_first_name=fake.first_name()
          d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_first_name}' WHERE rowid=={row_id}")
      else:
        # Generate with equivalences
        # Create table of equivalences
        equiv_table=next(tn_gen)
        d.sql(f"CREATE TABLE {equiv_table} ({basecolumn} VARCHAR, {newcolname} VARCHAR UNIQUE)")
        d.sql(f"INSERT INTO {equiv_table} ({basecolumn}) SELECT DISTINCT {basecolumn} FROM {table_name}")
        with tqdm(total=n_uniques, ) as progress:
          row_id=0
          while row_id<n_uniques:
            fake_name=fake.first_name()
            try:
              d.sql(f"UPDATE {equiv_table} SET {newcolname}='{fake_first_name}' WHERE rowid=={row_id}")
              row_id+=1
              progress.update(1)
            except d.duckdb.Error as e:
              pass
        return d.sql(f"SELECT COLUMNS({table_name}.*), {equiv_table}.{newcolname} AS {newcolname} FROM {table_name}, {equiv_table} WHERE {table_name}.{basecolumn}=={equiv_table}.{basecolumn}")
      table=d.sql(f"FROM {table_name}")
      return table
    else:
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
      all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
      for row_id in tqdm(range(all_count), disable=not verbose):
        fake_name=fake.first_name()
        d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_first_name}' WHERE rowid=={row_id}")
      table=d.sql(f"FROM {table_name}")
      return table
    return object
  return object

def add_syn_class_column(object, basecolumn=None, newcolname=None, maxuniques=1000, verbose=False):
  """
  Add a newcolname char column with equal-length LETTER classes. If basecolum is valid (is a column
  of the object) and the unique number of first identifiers in it are max maxunique. If the number of
  unique is greater than maxunique, it will only generate identifiers without trying to maintain an
  equivalence.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(newcolname, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    if isinstance(basecolumn,str):
      # Get unique names in basecolumn
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      n_uniques=d.sql(f"SELECT COUNT(DISTINCT {basecolumn}) FROM {table_name}").fetchall()[0][0]
      if n_uniques>maxuniques:
        # Generate without equivalences
        n_letters=letters_for(len(str(n_uniques)))
        d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
        all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
        for row_id in tqdm(range(all_count), disable=not verbose):
          fake_code=random_code(n_letters)
          d.sql(f"UPDATE {table_name} SET {newcolname}='{fake_code}' WHERE rowid=={row_id}")
      else:
        # Generate with equivalences
        # Create table of equivalences
        equiv_table=next(tn_gen)
        n_letters=letters_for(len(str(n_uniques)))
        d.sql(f"CREATE TABLE {equiv_table} ({basecolumn} VARCHAR, {newcolname} VARCHAR UNIQUE)")
        d.sql(f"INSERT INTO {equiv_table} ({basecolumn}) SELECT DISTINCT {basecolumn} FROM {table_name}")
        with tqdm(total=n_uniques, disable=not verbose) as progress:
          row_id=0
          while row_id<n_uniques:
            fake_code=random_code(n_letters)
            try:
              d.sql(f"UPDATE {equiv_table} SET {newcolname}='{fake_code}' WHERE rowid=={row_id}")
              row_id+=1
              progress.update(1)
            except d.duckdb.Error as e:
              pass
        return d.sql(f"SELECT COLUMNS({table_name}.*), {equiv_table}.{newcolname} AS {newcolname} FROM {table_name}, {equiv_table} WHERE {table_name}.{basecolumn}=={equiv_table}.{basecolumn}")
      table=d.sql(f"FROM {table_name}")
      return table
    else:
      table_name=next(tn_gen)
      table=object.to_table(table_name)
      d.sql(f"ALTER TABLE {table_name} ADD COLUMN {newcolname} VARCHAR")
      all_count=d.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall()[0][0]
      n_letters=letters_for(len(str(all_count)))
      for row_id in tqdm(range(all_count), disable=not verbose):
        fake_name=random_code(n_letters)
        d.sql(f"UPDATE {table_name} SET {newcolname}='{random_code}' WHERE rowid=={row_id}")
      table=d.sql(f"FROM {table_name}")
      return table
    return object
  return object

def syn_date_column(object, basecolumn, usebasecolumn=False, startdate=None, enddate=None, dateformat="%Y-%m-%d", verbose=False):
  """
  Replace all date values in basecolumn of object with synthetic generated dates. If startdate and 
  enddate are given, the generated dates are between these. If only one is given and usebasecolumn
  is True, then generate between given and basecolumn. If only one is given and usebasecolum is
  false, generate between today and the one given.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
    and isinstance(basecolumn, str):
    new_colname=f"{basecolumn}_syn"
    if startdate and enddate:
      rel=add_syn_date_column(object,basecolumn,newcolname=new_colname,startdate=startdate,enddate=enddate,dateformat=dateformat,verbose=verbose)
    elif startdate:
      if usebasecolumn:
        rel=add_syn_date_column(object,basecolumn,newcolname=new_colname,startdate=startdate,enddate=None,dateformat=dateformat,verbose=verbose)
      else:
        rel=add_syn_date_column(object,basecolumn,newcolname=new_colname,startdate=startdate,enddate=datetime.today(),dateformat=dateformat,verbose=verbose)
    elif enddate:
      if usebasecolumn:
        rel=add_syn_date_column(object,basecolumn,newcolname=new_colname,startdate=None,enddate=enddate,dateformat=dateformat,verbose=verbose)
      else:
        rel=add_syn_date_column(object,basecolumn,newcolname=new_colname,startdate=datetime.today(),enddate=enddate,dateformat=dateformat,verbose=verbose)
    table_name=next(tn_gen)
    table=rel.to_table(table_name)
    d.sql(f"UPDATE {table_name} SET {basecolumn}={new_colname}")
    d.sql(f"ALTER TABLE {table_name} DROP COLUMN {new_colname}")
    return d.sql(f"FROM {table_name}")
  return object

def syn_city_column(object, basecolumn=None, maxuniques=1000, verbose=False):
  """
  Replace all city values in basecolumn of object with random generated towns. User must check
  documentation of Faker python module because it generates real names using the country location
  and it only can generate a limited number of names (1036 town names for Colombia, es_CO).
  If the unique number of values in basecolumn is less than or equal than maxuniques, it can replace
  each unique value in source with unique values in replacement.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
  and isinstance(basecolumn, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    new_colname=f"{basecolumn}_generated"
    rel=add_syn_city_column(object, basecolumn=basecolumn, newcolname=new_colname, maxuniques=maxuniques, verbose=verbose)
    table_name=next(tn_gen)
    table=rel.to_table(table_name)
    d.sql(f"UPDATE {table_name} SET {basecolumn}={new_colname}")
    return d.sql(f"SELECT * EXCLUDE ({new_colname}) FROM {table_name}")
  return object

def syn_name_column(object, basecolumn=None, maxuniques=1000, verbose=False):
  """
  Replace all people name values in basecolumn of object with random generated people full name.
  User must check documentation of Faker python module because it generates "probable" names using
  the country location customs and it only can generate a limited number of names.
  If the unique number of values in basecolumn is less than or equal than maxuniques, it can replace
  each unique value in source with unique values in replacement.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
  and isinstance(basecolumn, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    new_colname=f"{basecolumn}_generated"
    rel=add_syn_name_column(object, basecolumn=basecolumn, newcolname=new_colname, maxuniques=maxuniques, verbose=verbose)
    table_name=next(tn_gen)
    table=rel.to_table(table_name)
    d.sql(f"UPDATE {table_name} SET {basecolumn}={new_colname}")
    return d.sql(f"SELECT * EXCLUDE ({new_colname}) FROM {table_name}")
  return object

def syn_last_name_column(object, basecolumn=None, maxuniques=1000, verbose=False):
  """
  Replace all people name values in basecolumn of object with random generated last names. User must
  check documentation of Faker python module because it generates "probable" names using the country 
  location customs and it only can generate a limited number of names.
  If the unique number of values in basecolumn is less than or equal than maxuniques, it can replace
  each unique value in source with unique values in replacement.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
  and isinstance(basecolumn, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    new_colname=f"{basecolumn}_generated"
    rel=add_syn_last_name_column(object, basecolumn=basecolumn, newcolname=new_colname, maxuniques=maxuniques, verbose=verbose)
    table_name=next(tn_gen)
    table=rel.to_table(table_name)
    d.sql(f"UPDATE {table_name} SET {basecolumn}={new_colname}")
    return d.sql(f"SELECT * EXCLUDE ({new_colname}) FROM {table_name}")
  return object

def syn_first_name_column(object, basecolumn=None, maxuniques=1000, verbose=False):
  """
  Replace all people name values in basecolumn of object with random generated first names. User must
  check documentation of Faker python module because it generates "probable" names using the country 
  location customs and it only can generate a limited number of names.
  If the unique number of values in basecolumn is less than or equal than maxuniques, it can replace
  each unique value in source with unique values in replacement.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
  and isinstance(basecolumn, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    new_colname=f"{basecolumn}_generated"
    rel=add_syn_first_name_column(object, basecolumn=basecolumn, newcolname=new_colname, maxuniques=maxuniques, verbose=verbose)
    table_name=next(tn_gen)
    table=rel.to_table(table_name)
    d.sql(f"UPDATE {table_name} SET {basecolumn}={new_colname}")
    return d.sql(f"SELECT * EXCLUDE ({new_colname}) FROM {table_name}")
  return object

def syn_class_column(object, basecolumn=None, maxuniques=1000, verbose=False):
  """
  Replace all text values in basecolumn of object with random generated text codes (all caps).
  If the unique number of values in basecolumn is less than or equal than maxuniques, it can replace
  each unique value in source with unique values in replacement.
  """
  if isinstance(object, d.duckdb.DuckDBPyRelation) \
  and isinstance(basecolumn, str):
    if "fake" in globals():
      global fake
    else:
      if "locale" in globals():
        global locale
      else:
        locale="es_CO"
      fake=Faker(locale)
      Faker.seed(42)
    new_colname=f"{basecolumn}_generated"
    rel=add_syn_class_column(object, basecolumn=basecolumn, newcolname=new_colname, maxuniques=maxuniques, verbose=verbose)
    table_name=next(tn_gen)
    table=rel.to_table(table_name)
    d.sql(f"UPDATE {table_name} SET {basecolumn}={new_colname}")
    return d.sql(f"SELECT * EXCLUDE ({new_colname}) FROM {table_name}")
  return object

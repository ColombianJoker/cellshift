from . import random_code
from math import ceil
from tqdm import tqdm
import mmap, os

def generate_kb_code(n_kb: int):
  """
  Generate a KB of random uppercase 32 letter codes
  """
  n_codes=(1024)//32
  kb_block=random_code(32)*n_codes
  return kb_block*n_kb

def generate_mb_code(n_mb: int):
  """
  Generate a MB of random uppercase 32 letter codes
  """
  n_codes=(1024*1024)//128
  mb_block=random_code(128)*n_codes
  return mb_block*n_mb

def get_file_size(filename: str):
  """
  Get file size of file in bytes
  """
  if isinstance(filename, str) and os.path.isfile(filename):
    return os.path.getsize(filename)
  else:
    return 0
  
def fast_overwrite(filename: str, verbose=False):
  """
  Fast overwrite of file with MMAP
  """
  if isinstance(filename, str) and os.path.isfile(filename):
    kb, mb = 1024, 1024*1024
    four_kb, four_mb = 4*kb, 4*mb
    file_size=get_file_size(filename)
    if file_size>0:
      with open(filename, "r+b") as f:
        # Map all file (0 is all file)
        mapped_file=mmap.mmap(f.fileno(),0)
        if file_size>=mb:
          # Overwrite it with MB blocks
          four_mb_code=generate_mb_code(4).encode()
          file_size_in_4mb=ceil(file_size/four_mb)
          for i in tqdm(range(file_size_in_4mb), disable=not verbose):
            block_start=i*four_mb
            block_end=(i+1)*four_mb
            if file_size>=block_end:
              mapped_file[block_start:block_end]=four_mb_code
            else:
              mapped_file[block_start:file_size]=four_mb_code[:(file_size%four_mb)]
            mapped_file.flush()
        else:
          four_kb_code=generate_kb_code(4).encode()
          file_size_in_4kb=ceil(file_size/(4*1024))
          for i in tqdm(range(file_size_in_4kb), disable=not verbose):
            mapped_file[i*four_kb:(i+1)*four_kb]=four_kb_code
          mapped_file.flush()
      return True
    else:
      return False
  return False

def destroy(filename: str, verbose=False):
  """
  Destroy, by overwriting and removing the given file
  """
  if isinstance(filename, str) and os.path.isfile(filename):
    if fast_overwrite(filename, verbose):
      try:
        os.unlink(filename)
        if verbose:
          print(f"{filename} destroyed.")
        return True
      except Exception as err:
        print(f"Error {err} on {filename}")
        return False
    else:
      if verbose:
        print(f"Could not overwrite {filename}")
      return False
  return False
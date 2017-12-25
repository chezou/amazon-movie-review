import gzip
import mmap
from tqdm import tqdm

def get_line_number(file_path):  
  fp = gzip.open(file_path, "r+")
  buf = mmap.mmap(fp.fileno(), 0)
  lines = 0
  while buf.readline():
      lines += 1
  return lines

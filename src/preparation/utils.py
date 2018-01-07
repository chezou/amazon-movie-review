import gzip
import sys

def get_line_number(file_path):
  sys.stderr.write("Counting line number of {}".format(file_path))

  f = gzip.open(file_path)
  lines = 0
  buf_size = 1024 * 1024
  read_f = f.read # loop optimization

  buf = read_f(buf_size)
  while buf:
    lines += buf.count('\n')
    buf = read_f(buf_size)

  return lines

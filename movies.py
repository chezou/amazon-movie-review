import gzip
import simplejson
import mmap
from tqdm import tqdm

def get_line_number(file_path):  
  fp = gzip.open(file_path, "r+")
  buf = mmap.mmap(fp.fileno(), 0)
  lines = 0
  while buf.readline():
      lines += 1
  return lines

def parse(filename):
  f = gzip.open(filename, 'r')
  entry = {}
  for l in f:
    l = l.strip()
    colonPos = l.find(':')
    if colonPos == -1:
      yield entry
      entry = {}
      continue
    eName = l[:colonPos]
    rest = l[colonPos+2:]
    entry[eName] = unicode(rest, errors='ignore')
  yield entry

file_path = "data/movies.txt.gz"

# tqdm doesn't work appropriately with gziped file
for e in tqdm(parse(file_path), total=get_line_number(file_path)):
  print(simplejson.dumps(e))

"""
Parser for Web data: Amazon movie reviews

https://snap.stanford.edu/data/web-Movies.html
"""

import gzip
import simplejson
from tqdm import tqdm
from utils import get_line_number

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

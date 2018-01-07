"""
Parser for Web data: Amazon movie reviews

https://snap.stanford.edu/data/web-Movies.html
"""

import gzip
import simplejson
from tqdm import tqdm
from utils import get_line_number

def parse(filename, total):
  f = gzip.open(filename, 'r')
  entry = {}
  for l in tqdm(f, total=total):
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


if __name__ == '__main__':
  file_path = "data/movies.txt.gz"
  line_num = get_line_number(file_path)

  for e in parse(file_path, total=line_num):
    print(simplejson.dumps(e))

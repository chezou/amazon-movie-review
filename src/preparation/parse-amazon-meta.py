"""
Parser for Amazon product co-purchasing network metadata
 
https://snap.stanford.edu/data/amazon-meta.html
"""

import re
import gzip
from tqdm import tqdm
from utils import get_line_number

def parse(filename, total):
  IGNORE_FIELDS = ['Total items', 'reviews']
  f = gzip.open(filename, 'r')
  entry = {}
  categories = []
  reviews = []
  similar_items = []
  
  for line in tqdm(f, total=total):
    line = line.strip()
    colonPos = line.find(':')

    if line.startswith("Id"):
      if reviews:
        entry["reviews"] = reviews
      if categories:
        entry["categories"] = categories
      yield entry
      entry = {}
      categories = []
      reviews = []
      rest = line[colonPos+2:]
      entry["id"] = unicode(rest.strip(), errors='ignore')
      
    elif line.startswith("similar"):
      similar_items = line.split()[2:]
      entry['similar_items'] = similar_items

    # "cutomer" is typo of "customer" in original data
    elif line.find("cutomer:") != -1:
      review_info = line.split()
      reviews.append({'customer_id': review_info[2], 
                      'rating': int(review_info[4]), 
                      'votes': int(review_info[6]), 
                      'helpful': int(review_info[8])})

    elif line.startswith("|"):
      categories.append(line)

    elif colonPos != -1:
      eName = line[:colonPos]
      rest = line[colonPos+2:]

      if not eName in IGNORE_FIELDS:
        entry[eName] = unicode(rest.strip(), errors='ignore')

  if reviews:
    entry["reviews"] = reviews
  if categories:
    entry["categories"] = categories
    
  yield entry


if __name__ == '__main__':
  file_path = "data/amazon-meta.txt.gz"

  import simplejson

  line_num = get_line_number(file_path)

  for e in parse(file_path, total=line_num):
    if e:
      print(simplejson.dumps(e))

'''
This is an example document.

example = """Id:   1
ASIN: 0827229534
  title: Patterns of Preaching: A Sermon Sampler
  group: Book
  salesrank: 396585
  similar: 5  0804215715  156101074X  0687023955  0687074231  082721619X
  categories: 2
   |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Christianity[12290]|Clergy[12360]|Prea
ching[12368]
   |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Christianity[12290]|Clergy[12360]|Serm
ons[12370]
  reviews: total: 2  downloaded: 2  avg rating: 5
    2000-7-28  cutomer: A2JW67OY8U6HHK  rating: 5  votes:  10  helpful:   9
    2003-12-14  cutomer: A2VE83MZF98ITY  rating: 5  votes:   6  helpful:   5

Id:   8
ASIN: 0231118597
  title: Losing Matt Shepard
  group: Book
  salesrank: 277409
  similar: 5  B000067D0Y  0375727191  080148605X  1560232579  0300089023
  categories: 4
   |Books[283155]|Subjects[1000]|Gay & Lesbian[301889]|Nonfiction[10703]|General[10716]
   |Books[283155]|Subjects[1000]|Nonfiction[53]|Crime & Criminals[11003]|Criminology[11005]
   |Books[283155]|Subjects[1000]|Nonfiction[53]|Politics[11079]|General[11083]
   |Books[283155]|Subjects[1000]|Nonfiction[53]|Politics[11079]|U.S.[11117]
  reviews: total: 15  downloaded: 15  avg rating: 4.5
    2000-10-31  cutomer: A2F1X6YFCJZ1FH  rating: 5  votes:  10  helpful:   9
    2000-11-1  cutomer: A1OZQCZAK21S6M  rating: 5  votes:  13  helpful:  12
    2000-11-19  cutomer:  AL5D52NA8F67F  rating: 5  votes:  16  helpful:  13
    2001-4-16  cutomer:  AVFBIM1W41IXO  rating: 1  votes:   0  helpful:   0
    2001-5-10  cutomer: A3I6SOXDIE0M8R  rating: 5  votes:   6  helpful:   6
    2001-7-1  cutomer: A3559TE3F9RRNL  rating: 5  votes:   5  helpful:   5
    2001-8-25  cutomer:  ASPUU0H77LFXG  rating: 5  votes:   3  helpful:   3
    2001-9-13  cutomer: A3L902U49A6X5K  rating: 5  votes:   6  helpful:   6
    2001-10-25  cutomer:  AL5OEDM8TPTKV  rating: 5  votes:  10  helpful:  10
    2001-11-3  cutomer: A1R64WON03GTN4  rating: 5  votes:   1  helpful:   1
    2001-12-24  cutomer: A2WKESDGF2YC8S  rating: 5  votes:   8  helpful:   8
    2001-12-31  cutomer:  A71P2O8OMF8GY  rating: 5  votes:   7  helpful:   5
    2002-4-9  cutomer:  AB8HLDYSDI5M7  rating: 4  votes:  10  helpful:   4
    2002-9-23  cutomer: A37FDCXZLI0MAC  rating: 2  votes:  12  helpful:   5
    2003-9-3  cutomer:  AQE41QO3NEUMW  rating: 5  votes:   3  helpful:   2
"""

'''

import sys
import re
import math

def extract_time(line):
  m = re.search("bench:\s*([\d,]*)\s*ns/iter", line)
  return m.group(1).replace(",", "")

def extract_key(line):
  raw = re.match("test\s*(\S*)\s", line).group(1)
  key = ""
  if raw.find("hamt") >= 0:
    key += "hamt-"
  if raw.find("hashmap") >= 0:
    key += "hashmap-"

  if raw.find("find") >= 0:
    key += "find-"
  if raw.find("insert") >= 0:
    key += "insert-"

  if raw.find("clone") >= 0:
    key += "clone-"

  key += re.match("\D*(\d*)", raw).group(1)
  return key

def should_ignore(line):
  return ((line.find("share") >= 0) or
          (line.find("remove") >= 0) or
          (line.find("iterate") >= 0) or
          (line.find("ns/iter") < 0))

# Parse the timings
timings = {}
for line in sys.stdin.readlines():
  print(line.strip())

  if should_ignore(line):
    continue

  time = extract_time(line)
  if time:
    key = extract_key(line)
    timings[key] = int(float(time) * 0.001 + 0.5)
    print(key + ": " + time + " -> " + str(timings[key]))


hamt_find_10 = timings["hamt-find-10"]
hamt_find_1000 = timings["hamt-find-1000"]
hamt_find_100000 = timings["hamt-find-100000"]

hashmap_find_10 = timings["hashmap-find-10"]
hashmap_find_1000 = timings["hashmap-find-1000"]
hashmap_find_100000 = timings["hashmap-find-100000"]

hamt_find_10_percent = int((100 * hamt_find_10) / hashmap_find_10)
hamt_find_1000_percent = int((100 * hamt_find_1000) / hashmap_find_1000)
hamt_find_100000_percent = int((100 * hamt_find_100000) / hashmap_find_100000)

hamt_insert_10 = timings["hamt-insert-10"]
hamt_insert_1000 = timings["hamt-insert-1000"]
hamt_insert_100000 = timings["hamt-insert-100000"]

hashmap_insert_10 = timings["hashmap-insert-10"] - timings["hashmap-clone-10"]
hashmap_insert_1000 = timings["hashmap-insert-1000"] - timings["hashmap-clone-1000"]
hashmap_insert_100000 = timings["hashmap-insert-100000"] - timings["hashmap-clone-100000"]

hamt_insert_10_percent = int((100 * hamt_insert_10) / hashmap_insert_10)
hamt_insert_1000_percent = int((100 * hamt_insert_1000) / hashmap_insert_1000)
hamt_insert_100000_percent = int((100 * hamt_insert_100000) / hashmap_insert_100000)

# Print FIND table (microseconds)
print(
"""
| ELEMENT COUNT | HAMT    | HASHMAP |
|:--------------|:-------:|:-------:|
| 10            | {0:>7d} | {3:>7d} |
| 1000          | {1:>7d} | {4:>7d} |
| 100000        | {2:>7d} | {5:>7d} |
""".format(hamt_find_10,
           hamt_find_1000,
           hamt_find_100000,
           hashmap_find_10,
           hashmap_find_1000,
           hashmap_find_100000))

# Print FIND table (percent)
print(
"""
| ELEMENT COUNT | HAMT     | HASHMAP  |
|:--------------|:--------:|:--------:|
| 10            | {0:>7d}% | {3:>7d}% |
| 1000          | {1:>7d}% | {4:>7d}% |
| 100000        | {2:>7d}% | {5:>7d}% |
""".format(hamt_find_10_percent,
           hamt_find_1000_percent,
           hamt_find_100000_percent,
           100,
           100,
           100))

# Print INSERT table (microseconds)
print(
"""
| ELEMENT COUNT | HAMT    | HASHMAP |
|:--------------|:-------:|:-------:|
| 10            | {0:>7d} | {3:>7d} |
| 1000          | {1:>7d} | {4:>7d} |
| 100000        | {2:>7d} | {5:>7d} |
""".format(hamt_insert_10,
           hamt_insert_1000,
           hamt_insert_100000,
           hashmap_insert_10,
           hashmap_insert_1000,
           hashmap_insert_100000))

# Print INSERT table (percent)
print(
"""
| ELEMENT COUNT | HAMT     | HASHMAP  |
|:--------------|:--------:|:--------:|
| 10            | {0:>7d}% | {3:>7d}% |
| 1000          | {1:>7d}% | {4:>7d}% |
| 100000        | {2:>7d}% | {5:>7d}% |
""".format(hamt_insert_10_percent,
           hamt_insert_1000_percent,
           hamt_insert_100000_percent,
           100,
           100,
           100))

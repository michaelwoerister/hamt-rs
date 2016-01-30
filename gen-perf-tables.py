# Copyright (c) 2013, 2014, 2015, 2016 Michael Woerister
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

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
    timings[key] = int(time)
    print("timings[" + key + "] = " + str(timings[key]))


def to_micro_secs(t):
  return int(float(t) * 0.001 + 0.5)

hamt_find_10 = to_micro_secs(timings["hamt-find-10"])
hamt_find_1000 = to_micro_secs(timings["hamt-find-1000"])
hamt_find_100000 = to_micro_secs(timings["hamt-find-100000"])

hashmap_find_10 = to_micro_secs(timings["hashmap-find-10"])
hashmap_find_1000 = to_micro_secs(timings["hashmap-find-1000"])
hashmap_find_100000 = to_micro_secs(timings["hashmap-find-100000"])

hamt_find_10_percent = int((100 * timings["hamt-find-10"]) / timings["hashmap-find-10"])
hamt_find_1000_percent = int((100 * timings["hamt-find-1000"]) / timings["hashmap-find-1000"])
hamt_find_100000_percent = int((100 * timings["hamt-find-100000"]) / timings["hashmap-find-100000"])

hamt_insert_10 = to_micro_secs(timings["hamt-insert-10"])
hamt_insert_1000 = to_micro_secs(timings["hamt-insert-1000"])
hamt_insert_100000 = to_micro_secs(timings["hamt-insert-100000"])

hashmap_insert_10_nanos = timings["hashmap-insert-10"] - timings["hashmap-clone-10"]
hashmap_insert_1000_nanos = timings["hashmap-insert-1000"] - timings["hashmap-clone-1000"]
hashmap_insert_100000_nanos = timings["hashmap-insert-100000"] - timings["hashmap-clone-100000"]

hashmap_insert_10 = to_micro_secs(hashmap_insert_10_nanos)
hashmap_insert_1000 = to_micro_secs(hashmap_insert_1000_nanos)
hashmap_insert_100000 = to_micro_secs(hashmap_insert_100000_nanos)

hamt_insert_10_percent = int((100 * timings["hamt-insert-10"]) / hashmap_insert_10_nanos)
hamt_insert_1000_percent = int((100 * timings["hamt-insert-1000"]) / hashmap_insert_1000_nanos)
hamt_insert_100000_percent = int((100 * timings["hamt-insert-100000"]) / hashmap_insert_100000_nanos)


print(
"""
FIND (microseconds)

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

print(
"""
FIND (percent)

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

print(
"""
INSERT (microseconds)

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

print(
"""
INSERT (percent)

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

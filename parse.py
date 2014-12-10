import re
import sys
import io
import json

def tokenize(line):
      headers = []
      lasti = ""
      buf = ""
      for i in line:
        if i == " " and lasti == " ":
          if buf.strip() <> '':
            headers.append(buf.strip())
          buf = ""
        buf += i
        lasti = i
      headers.append(buf.strip())
      return headers

class NodetoolUtil:
  def parse_cfhistograms(self, lines):
   data = {}
   ind = 0
   for line in lines:
    if ind == 1:
      # headers (Percentile  SSTables     Write Latency      Read Latency    Partition Size        Cell Count)
      headers = [h.replace(" ", "_").lower() for h in tokenize(line)[1:]]
      #data["headers"] = headers
      for h in headers:
        data[h] = {}
    elif ind > 2:
      vals = tokenize(line)
      #data["vals"] = vals
      if len(vals) == 6:
        for i, h in enumerate(headers):
        #for i, h in data.items():
          #print i, h, vals[i+1]
          v = float(vals[i+1])
          if h.endswith("latency"):
            v = round(v / 1000.0, 2)
          data[h][vals[0]] = v
    ind += 1
   return data

  def parse_cfstats(self, lines):
   current_keyspace = ""
   current_table = ""
   for line in lines:
    mk = re.match("Keyspace: ([a-zA-Z0-9_]+)", line)
    mt = re.match("Table: ([a-zA-Z0-9_]+)", line)
    if mk:
      #print "Found keyspace: ", mk.group(1)
      current_keyspace = mk.group(1)
      current_table = ""
      data[current_keyspace] = {}
      data[current_keyspace]["tables"] = {}
    elif mt:
      #print "Found table: ", mt.group(1)
      current_table = mt.group(1)
      data[current_keyspace]["tables"][current_table] = {}
    else:
      #print line
      m = re.match("([a-zA-Z0-9_ ]+): ([0-9\.]+)[ .*]*", line)
      if m:
        k = m.group(1).replace(" ", "_").lower()
        v = m.group(2)
        if v is not None: v = float(v)
        if current_table <> '':
          data[current_keyspace]["tables"][current_table][k] = v
        elif current_keyspace <> '':
          data[current_keyspace][k] = v
   return data


u = NodetoolUtil()

lines = []
with open("cfhistograms_timeline_device.txt", "r") as f:
  for l in f:
    line = l.strip()
    lines.append(line)
data = u.parse_cfhistograms(lines)
print json.dumps(data)

lines = []
with open("cfstats.txt", "r") as f:
  for l in f:
    line = l.strip()
    lines.append(line)
data = u.parse_cfstats(lines)
print json.dumps(data)


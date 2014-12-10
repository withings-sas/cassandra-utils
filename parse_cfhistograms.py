import re
import sys
import io
import json

current_keyspace = ""
current_table = ""

data = {}
ind = 0

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

with open("cfhistograms_timeline_device.txt", "r") as f:
  for l in f:
    line = l.strip()
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

print json.dumps(data)


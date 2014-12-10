import re
import sys
import io
import json

current_keyspace = ""
current_table = ""

data = {}

with open("cfstats.txt", "r") as f:
  for l in f:
    line = l.strip()

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

print json.dumps(data)


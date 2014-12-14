import re
import io
import sys
import json
import subprocess
from optparse import OptionParser

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
	def cfhistograms(self, keyspace, table):
		nodetool_cmd = "nodetool cfhistograms %s %s" % (keyspace, table)
		lines = subprocess.check_output(nodetool_cmd, shell=True).split("\n")
		return self.parse_cfhistograms(lines)

	def parse_cfhistograms(self, lines):
		data = {}
		ind = 0
		for l in lines:
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
						v = float(vals[i+1])
						if h.endswith("latency"):
							v = round(v / 1000.0, 2)
						data[h][vals[0]] = v
			ind += 1
		return data

	def cfstats(self):
		nodetool_cmd = "nodetool cfstats"
		lines = subprocess.check_output(nodetool_cmd, shell=True).split("\n")
		return self.parse_cfstats(lines)

	def parse_cfstats(self, lines):
		data = {}
		current_keyspace = ""
		current_table = ""
		for l in lines:
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
					if v is not None: v = round(float(v), 3) if "." in v else int(v)
					if current_table <> '':
						data[current_keyspace]["tables"][current_table][k] = v
					elif current_keyspace <> '':
						data[current_keyspace][k] = v
		return data


if __name__ == '__main__':
	lines = sys.stdin

	parser = OptionParser()
	parser.add_option("-t", "--type", dest="type", help="nodetool output type")

	(options, args) = parser.parse_args()

	u = NodetoolUtil()
	if options.type == "cfhistograms":
		data = u.parse_cfhistograms(lines)
	elif options.type == "cfstats":
		data = u.parse_cfstats(lines)
	else:
		print parser.print_help()
		sys.exit(0)

	print json.dumps(data)


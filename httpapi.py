#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import daemon
import lockfile
import nodetoolutil
import bottle
from bottle import route, run


@route('/cfhistograms/:keyspace/:table')
def cfhistograms(keyspace, table):
	u = nodetoolutil.NodetoolUtil()
	data = u.cfhistograms(keyspace, table)
	return json.dumps(data)

@route('/cfstats')
def cfstats():
	u = nodetoolutil.NodetoolUtil()
	data = u.cfstats()
	return json.dumps(data)

@route('/stats')
def stats():
	u = nodetoolutil.NodetoolUtil()
	data = {
		'cfstats': u.cfstats(),
		'tpstats': {} #u.tpstats()
	}
	return json.dumps(data)


if __name__ == '__main__':
	cwd = os.path.dirname(os.path.realpath(__file__))
	with open('/var/log/nodetoolutil_access_log', 'a') as log:
		with daemon.DaemonContext(stdin=None, stdout=log, stderr=log, working_directory=cwd, pidfile=lockfile.FileLock('/var/run/nodetoolutil.pid')):
			run(host='0.0.0.0', port=8080)

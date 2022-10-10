#!/usr/bin/env python3
# -*- coding: utf-8 -*

import sys
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

graph = Graph()
hostname = sys.argv[1]
remoteConn = DriverRemoteConnection('ws://' + hostname + '/gremlin','g')
g = graph.traversal().withRemote(remoteConn)

res = g.V().limit(2).count().next()
assert res == 2
remoteConn.close()

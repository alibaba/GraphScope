#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from neo4j import GraphDatabase
from neo4j import Session as Neo4jSession

import argparse

def count_vertices(sess: Neo4jSession):
    query = "MATCH (n) RETURN COUNT(n);"
    result = sess.run(query)
    for record in result:
        print(record[0])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count the number of vertices in a graph.")
    parser.add_argument("--endpoint", type=str, required=True, help="The endpoint to connect.")
    args = parser.parse_args()

    driver = GraphDatabase.driver(args.endpoint, auth=None)
    with driver.session() as session:
        count_vertices(session)
    driver.close()
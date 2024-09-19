#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited. All Rights Reserved.
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

import kuzu
import sys

def execute_cypher_file(conn, file_path):
    with open(file_path, 'r') as file:
        for line in file:
            statement = line.strip()
            if statement:
                conn.execute(statement)

def main(db_path, schema_path, dataloading_path):
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)

    print("start inserting")

    # Create schema
    execute_cypher_file(conn, schema_path)

    # Insert data
    execute_cypher_file(conn, dataloading_path)

    print("start querying")

    # Execute Cypher query
    response = conn.execute(
        """
        MATCH (a:NAME)
        RETURN count(a)
        """
    )

    while response.has_next():
        print(response.get_next())

    print("query end")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <db_path> <schema_path> <dataloading_path>")
    else:
        db_path = sys.argv[1]
        schema_path = sys.argv[2]
        dataloading_path = sys.argv[3]
        main(db_path, schema_path, dataloading_path)
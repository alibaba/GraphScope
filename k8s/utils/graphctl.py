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

import json
import sys


def spawn_vineyard_io_stream(
    source: str,
    vineyard_ipc_socket: str,
    hosts: list,
):
    import vineyard
    import vineyard.io

    stream_id = repr(
        vineyard.io.open(
            source,
            mode="r",
            vineyard_endpoint="0.0.0.0:9600",
            vineyard_ipc_socket=vineyard_ipc_socket,
            hosts=hosts,
            deployment="kubernetes",
        )
    )
    return "vineyard", stream_id


def maybe_ingest_to_vineyard(source, socket, hosts):
    if "://" in source:
        protocol = source.split("://")[0]
    else:
        protocol = "file"

    if (
        protocol in ("hdfs", "hive", "oss", "s3")
        or protocol == "file"
        and (
            source.endswith(".orc")
            or source.endswith(".parquet")
            or source.endswith(".pq")
        )
    ):
        new_protocol, new_source = spawn_vineyard_io_stream(source, socket, hosts)
        print(
            f"original uri = {source}, new_protocol = {new_protocol}, new_source = {new_source}"
        )
        return f"{new_protocol}://{new_source}"
    else:
        return source


def replace_data_path(json_str, socket, hosts):
    json_obj = json.loads(json_str)
    for vertex in json_obj["vertices"]:
        vertex["data_path"] = maybe_ingest_to_vineyard(
            vertex["data_path"], socket, hosts
        )
    for edge in json_obj["edges"]:
        edge["data_path"] = maybe_ingest_to_vineyard(edge["data_path"], socket, hosts)
    return json.dumps(json_obj)


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(
            """
Usage: python3 grootctl.py vineyard_ipc_socket hosts source_cfg dest_cfg
For example: python3 grootctl.py ./sock name1,name2 config config.new"""
        )
        sys.exit(1)

    ipc_socket = sys.argv[1]
    hosts = sys.argv[2]
    hosts = hosts.split(",")
    source_cfg = sys.argv[3]
    dest_cfg = sys.argv[4]
    with open(source_cfg) as f:
        config_str = f.read()
    new_config_str = replace_data_path(config_str, ipc_socket, hosts)
    with open(dest_cfg, "w") as f:
        f.write(new_config_str)
    print("Ingest data to vineyard success!")

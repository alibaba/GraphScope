#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited.
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

import hashlib
import json

from gscoordinator.flex.core.config import BASEID
from gscoordinator.version import __version__


def convert_to_configini(graph, ds_manager, config):
    # for bulk loader to connect to groot
    gremlin_interface = graph.gremlin_interface
    # column mapping config
    column_mapping_config = {}
    # project
    project = None
    # output table, deprecated
    output_table = f"{graph.name}_tmp"
    # odps volumn part spec,  0-31, A-Z, a-z, 0-9 and "_"
    odpsVolumePartspec = graph.name
    # traverse vertices
    for v in config["vertices"]:
        # vertex label
        vertex_type = v["type_name"]
        # vertex data source
        datasource = ds_manager.get_vertex_datasource(graph.id, vertex_type)
        # location: odps://project/table|ds=1,ps=2
        location = datasource["inputs"][0]
        # project
        project = location.split("/")[-2]
        # table|ds=1/ps=2
        table = location.split("/")[-1].replace(",", "/")
        # {"0": "id", "1": "name", "2": "age"}
        property_mapping = {}
        for column_mapping in datasource["column_mappings"]:
            property_mapping[str(column_mapping["column"]["index"])] = column_mapping[
                "property"
            ]
        column_mapping_config[table] = {
            "label": vertex_type,
            "srcLabel": None,
            "dstLabel": None,
            "srcPkColMap": None,
            "dstPkColMap": None,
            "propertiesColMap": property_mapping,
        }
        odpsVolumePartspec = f"{odpsVolumePartspec}_{vertex_type}"
    # traverse edges
    for e in config["edges"]:
        edge_type = "{}_{}_{}".format(
            e["source_vertex"], e["type_name"], e["destination_vertex"]
        )
        # edge data source
        datasource = ds_manager.get_edge_datasource(
            graph.id,
            e["type_name"],
            e["source_vertex"],
            e["destination_vertex"],
        )
        # location: odps://project/table|ds=1,ps=2
        location = datasource["inputs"][0]
        # project
        project = location.split("/")[-2]
        # table|ds=1/ps=2
        table = location.split("/")[-1].replace(",", "/")
        # {"0": "id"}
        source_pk_column_map = {}
        for column_mapping in datasource["source_vertex_mappings"]:
            source_pk_column_map[
                str(column_mapping["column"]["index"])
            ] = column_mapping["property"]
        # {"1": "id"}
        destination_pk_column_map = {}
        for column_mapping in datasource["destination_vertex_mappings"]:
            destination_pk_column_map[
                str(column_mapping["column"]["index"])
            ] = column_mapping["property"]
        # {"2": "weight", "3": "edge_id"}
        property_mapping = {}
        for column_mapping in datasource["column_mappings"]:
            property_mapping[str(column_mapping["column"]["index"])] = column_mapping[
                "property"
            ]
        column_mapping_config[table] = {
            "label": e["type_name"],
            "srcLabel": e["source_vertex"],
            "dstLabel": e["destination_vertex"],
            "srcPkColMap": source_pk_column_map,
            "dstPkColMap": destination_pk_column_map,
            "propertiesColMap": property_mapping,
        }
        odpsVolumePartspec = f"{odpsVolumePartspec}_{edge_type}"
    # hash to meet the odps limitation
    hashed = hashlib.sha256(odpsVolumePartspec.encode()).hexdigest()
    odpsVolumePartspec = hashed[:31]
    # custom_config
    custom_config = {
        "separatr": "\\\\|",  # fixed
        "graphEndpoint": gremlin_interface["grpc_endpoint"],
        "project": project,
        "outputTable": output_table,
        "columnMappingConfig": json.dumps(column_mapping_config),
        "authUsername": gremlin_interface["username"],
        "authPassword": gremlin_interface["password"],
        "dataSinkType": "volume",
        "odpsVolumeProject": project,
        # "-" is not allowed
        # https://aliyuque.antfin.com/computing-plggatform-doc/mc/zf9izr#ad082a24
        "odpsVolumeName": "gs_portal_" + graph.name.replace("-", "_"),
        "odpsVolumePartspec": odpsVolumePartspec,
    }
    # configini
    configini = {
        "instanceName": graph.name,
        "baseId": BASEID,
        "project": project,
        "dataSource": "ODPS",
        "label": "deprecated",
        "version": __version__,
        "customConfig": custom_config,
    }
    return configini

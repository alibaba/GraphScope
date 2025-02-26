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

# get the directory of the current file
import copy
import os
import time

import pytest
from neo4j import Session as Neo4jSession

from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import CreateGraphRequest
from gs_interactive.models import CreateProcedureRequest
from gs_interactive.models import GetGraphSchemaResponse
from gs_interactive.models import SchemaMapping
from gs_interactive.models import StartServiceRequest
from gs_interactive.models import UpdateProcedureRequest

cur_dir = os.path.dirname(os.path.abspath(__file__))
MODERN_GRAPH_DATA_DIR = os.path.abspath(
    os.path.join(cur_dir, "../../../../examples/modern_graph")
)
GRAPH_ALGO_DATA_DIR = os.path.abspath(
    os.path.join(cur_dir, "../../../../examples/graph_algo")
)
print("MODERN_GRAPH_DATA_DIR: ", MODERN_GRAPH_DATA_DIR)
print("GRAPH_ALGO_DATA_DIR: ", GRAPH_ALGO_DATA_DIR)

modern_graph_full = {
    "name": "full_graph",
    "description": "This is a test graph",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "software",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "lang",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
        ],
        "edge_types": [
            {
                "type_name": "knows",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
                "primary_keys": [],
            },
            {
                "type_name": "created",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "software",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
                "primary_keys": [],
            },
        ],
    },
}

modern_graph_multiple_edge_properties = {
    "name": "full_graph",
    "description": "This is a test graph",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "software",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "lang",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
        ],
        "edge_types": [
            {
                "type_name": "knows",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
                "primary_keys": [],
            },
            {
                "type_name": "created",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "software",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    },
                    {
                        "property_name": "since",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": [],
            },
        ],
    },
}

modern_graph_vertex_only = {
    "name": "vertex_only",
    "description": "This is a test graph, only contains vertex",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            }
        ],
        "edge_types": [],
    },
}

modern_graph_vertex_only_temporal = {
    "name": "vertex_only",
    "description": "This is a test graph, only contains vertex",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                    {
                        "property_name": "birthday",
                        "property_type": {"temporal": {"timestamp": ""}},
                    },
                ],
                "primary_keys": ["id"],
            }
        ],
        "edge_types": [],
    },
}

modern_graph_partial = {
    "name": "partial_graph",
    "description": "This is a test graph",
    "schema": {
        "vertex_types": [
            {
                "type_name": "person",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "age",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                ],
                "primary_keys": ["id"],
            }
        ],
        "edge_types": [
            {
                "type_name": "knows",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "person",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "weight",
                        "property_type": {"primitive_type": "DT_DOUBLE"},
                    }
                ],
                "primary_keys": [],
            }
        ],
    },
}


modern_graph_full_import_config = {
    "loading_config": {
        "data_source": {"scheme": "file", "location": "@" + MODERN_GRAPH_DATA_DIR},
        "import_option": "init",
        "format": {
            "type": "csv",
            "metadata": {
                "delimiter": "|",
            },
        },
    },
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["person.csv"],
        },
        {
            "type_name": "software",
            "inputs": ["software.csv"],
        },
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "knows",
                "source_vertex": "person",
                "destination_vertex": "person",
            },
            "inputs": ["person_knows_person.csv"],
        },
        {
            "type_triplet": {
                "edge": "created",
                "source_vertex": "person",
                "destination_vertex": "software",
            },
            "inputs": ["person_created_software.csv"],
        },
    ],
}


modern_graph_partial_import_config = {
    "loading_config": {
        "data_source": {"scheme": "file", "location": "@" + MODERN_GRAPH_DATA_DIR},
        "import_option": "init",
        "format": {
            "type": "csv",
            "metadata": {
                "delimiter": "|",
            },
        },
    },
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["person.csv"],
        },
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "knows",
                "source_vertex": "person",
                "destination_vertex": "person",
            },
            "inputs": ["person_knows_person.csv"],
        }
    ],
}

modern_graph_vertex_only_import_config = {
    "loading_config": {
        "data_source": {"scheme": "file", "location": "@" + MODERN_GRAPH_DATA_DIR},
        "import_option": "init",
        "format": {
            "type": "csv",
            "metadata": {
                "delimiter": "|",
            },
        },
    },
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["person.csv"],
        }
    ],
}

graph_algo_graph = {
    "name": "graph_algo",
    "version": "v0.1",
    "schema": {
        "vertex_types": [
            {
                "type_name": "Paper",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "conference",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "CCFRank",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "CCFField",
                        "property_type": {"string": {"long_text": ""}},
                    },
                    {
                        "property_name": "year",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                    {
                        "property_name": "paper",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "Challenge",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "challenge",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "Topic",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "topic",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "Task",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "task",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "Solution",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "solution",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "CCFField",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"primitive_type": "DT_SIGNED_INT64"},
                    },
                    {
                        "property_name": "field",
                        "property_type": {"string": {"long_text": ""}},
                    },
                ],
                "primary_keys": ["id"],
            },
        ],
        "edge_types": [
            {
                "type_name": "WorkOn",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "Paper",
                        "destination_vertex": "Task",
                        "relation": "MANY_TO_ONE",
                    }
                ],
            },
            {
                "type_name": "Resolve",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "Paper",
                        "destination_vertex": "Challenge",
                        "relation": "MANY_TO_MANY",
                    }
                ],
            },
            {
                "type_name": "Target",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "Task",
                        "destination_vertex": "Challenge",
                        "relation": "MANY_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_name": "number",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    }
                ],
            },
            {
                "type_name": "Belong",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "Task",
                        "destination_vertex": "Topic",
                        "relation": "MANY_TO_ONE",
                    }
                ],
            },
            {
                "type_name": "Use",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "Paper",
                        "destination_vertex": "Solution",
                        "relation": "MANY_TO_MANY",
                    }
                ],
            },
            {
                "type_name": "ApplyOn",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "Solution",
                        "destination_vertex": "Challenge",
                        "relation": "MANY_TO_ONE",
                    }
                ],
            },
            {
                "type_name": "HasField",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "Paper",
                        "destination_vertex": "CCFField",
                        "relation": "MANY_TO_MANY",
                    }
                ],
            },
            {
                "type_name": "Citation",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "Paper",
                        "destination_vertex": "Paper",
                        "relation": "MANY_TO_MANY",
                    }
                ],
            },
        ],
    },
}

graph_algo_graph_import_config = {
    "graph": "graph_algo",
    "loading_config": {
        "data_source": {"scheme": "file", "location": "@" + GRAPH_ALGO_DATA_DIR},
        "import_option": "init",
        "format": {
            "type": "csv",
            "metadata": {
                "delimiter": "|",
            },
        },
    },
    "vertex_mappings": [
        {"type_name": "Paper", "inputs": ["Paper.csv"]},
        {"type_name": "Challenge", "inputs": ["Challenge.csv"]},
        {"type_name": "Topic", "inputs": ["Topic.csv"]},
        {"type_name": "Task", "inputs": ["Task.csv"]},
        {"type_name": "Solution", "inputs": ["Solution.csv"]},
        {"type_name": "CCFField", "inputs": ["CCFField.csv"]},
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "WorkOn",
                "source_vertex": "Paper",
                "destination_vertex": "Task",
            },
            "inputs": ["Paper_WorkOn_Task.csv"],
        },
        {
            "type_triplet": {
                "edge": "Resolve",
                "source_vertex": "Paper",
                "destination_vertex": "Challenge",
            },
            "inputs": ["Paper_Resolve_Challenge.csv"],
        },
        {
            "type_triplet": {
                "edge": "Target",
                "source_vertex": "Task",
                "destination_vertex": "Challenge",
            },
            "source_vertex_mappings": [{"column": {"index": 0, "name": "id"}}],
            "destination_vertex_mappings": [{"column": {"index": 1, "name": "id"}}],
            "inputs": ["Task_Target_Challenge.csv"],
        },
        {
            "type_triplet": {
                "edge": "Belong",
                "source_vertex": "Task",
                "destination_vertex": "Topic",
            },
            "inputs": ["Task_Belong_Topic.csv"],
        },
        {
            "type_triplet": {
                "edge": "Use",
                "source_vertex": "Paper",
                "destination_vertex": "Solution",
            },
            "inputs": ["Paper_Use_Solution.csv"],
        },
        {
            "type_triplet": {
                "edge": "ApplyOn",
                "source_vertex": "Solution",
                "destination_vertex": "Challenge",
            },
            "inputs": ["Solution_ApplyOn_Challenge.csv"],
        },
        {
            "type_triplet": {
                "edge": "HasField",
                "source_vertex": "Paper",
                "destination_vertex": "CCFField",
            },
            "inputs": ["Paper_HasField_CCFField.csv"],
        },
        {
            "type_triplet": {
                "edge": "Citation",
                "source_vertex": "Paper",
                "destination_vertex": "Paper",
            },
            "inputs": ["Paper_Citation_Paper.csv"],
        },
    ],
}


new_graph_algo = {
    "name": "graph_algo",
    "schema": {
        "vertex_types": [
            {
                "type_name": "Challenge",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "description",
                        "property_type": {"string": {"long_text": None}},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "Task",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "description",
                        "property_type": {"string": {"long_text": None}},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "Solution",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "name",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "description",
                        "property_type": {"string": {"long_text": None}},
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "type_name": "Paper",
                "properties": [
                    {
                        "property_name": "id",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "published",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "year",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                    {
                        "property_name": "month",
                        "property_type": {"primitive_type": "DT_SIGNED_INT32"},
                    },
                    {
                        "property_name": "title",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "authors",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "summary",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "journal_ref",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "doi",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "primary_category",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "categories",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "problem_def",
                        "property_type": {"string": {"long_text": None}},
                    },
                    {
                        "property_name": "keywords",
                        "property_type": {"string": {"long_text": None}},
                    },
                ],
                "primary_keys": ["id"],
            },
        ],
        "edge_types": [
            {
                "type_name": "SolvedBy",
                "properties": [],
                "vertex_type_pair_relations": [
                    {
                        "destination_vertex": "Solution",
                        "relation": "MANY_TO_MANY",
                        "source_vertex": "Challenge",
                    }
                ],
            },
            {
                "type_name": "Cite",
                "properties": [],
                "vertex_type_pair_relations": [
                    {
                        "destination_vertex": "Paper",
                        "relation": "MANY_TO_MANY",
                        "source_vertex": "Paper",
                    }
                ],
            },
            {
                "type_name": "Has",
                "properties": [],
                "vertex_type_pair_relations": [
                    {
                        "destination_vertex": "Challenge",
                        "relation": "MANY_TO_MANY",
                        "source_vertex": "Paper",
                    }
                ],
            },
            {
                "type_name": "WorkOn",
                "properties": [],
                "vertex_type_pair_relations": [
                    {
                        "destination_vertex": "Task",
                        "relation": "MANY_TO_MANY",
                        "source_vertex": "Paper",
                    }
                ],
            },
            {
                "type_name": "Use",
                "properties": [],
                "vertex_type_pair_relations": [
                    {
                        "destination_vertex": "Solution",
                        "relation": "MANY_TO_MANY",
                        "source_vertex": "Paper",
                    }
                ],
            },
        ],
    },
}

"""
Replace the source location with the real location
"""
new_graph_algo_import_config = {
    "graph": "graph_algo",
    "loading_config": {
        "data_source": {"scheme": "file"},
        "import_option": "init",
        "format": {
            "type": "csv",
            "metadata": {
                "delimiter": "|",
                "header_row": True,
                "quoting": True,
                "quote_char": '"',
                "double_quote": True,
                "batch_reader": True,
            },
        },
    },
    "vertex_mappings": [
        {
            "type_name": "Challenge",
            "inputs": ["Challenge.csv"],
        },
        {"type_name": "Task", "inputs": ["Task.csv"]},
        {
            "type_name": "Solution",
            "inputs": ["Solution.csv"],
        },
        {"type_name": "Paper", "inputs": ["Paper.csv"]},
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "SolvedBy",
                "source_vertex": "Challenge",
                "destination_vertex": "Solution",
            },
            "inputs": ["Challenge_Solvedby_Solution.csv"],
            "column_mappings": [],
            "source_vertex_mappings": [
                {"column": {"index": 0, "name": "source"}, "property": "id"}
            ],
            "destination_vertex_mappings": [
                {"column": {"index": 1, "name": "target"}, "property": "id"}
            ],
        },
        {
            "type_triplet": {
                "edge": "Cite",
                "source_vertex": "Paper",
                "destination_vertex": "Paper",
            },
            "inputs": ["Paper_Cite_Paper.csv"],
            "column_mappings": [],
            "source_vertex_mappings": [
                {"column": {"index": 0, "name": "source"}, "property": "id"}
            ],
            "destination_vertex_mappings": [
                {"column": {"index": 1, "name": "target"}, "property": "id"}
            ],
        },
        {
            "type_triplet": {
                "edge": "Has",
                "source_vertex": "Paper",
                "destination_vertex": "Challenge",
            },
            "inputs": ["Paper_Has_Challenge.csv"],
            "column_mappings": [],
            "source_vertex_mappings": [
                {"column": {"index": 0, "name": "source"}, "property": "id"}
            ],
            "destination_vertex_mappings": [
                {"column": {"index": 1, "name": "target"}, "property": "id"}
            ],
        },
        {
            "type_triplet": {
                "edge": "WorkOn",
                "source_vertex": "Paper",
                "destination_vertex": "Task",
            },
            "inputs": ["Paper_WorkOn_Task.csv"],
            "column_mappings": [],
            "source_vertex_mappings": [
                {"column": {"index": 0, "name": "source"}, "property": "id"}
            ],
            "destination_vertex_mappings": [
                {"column": {"index": 1, "name": "target"}, "property": "id"}
            ],
        },
        {
            "type_triplet": {
                "edge": "Use",
                "source_vertex": "Paper",
                "destination_vertex": "Solution",
            },
            "inputs": ["Paper_Use_Solution.csv"],
            "column_mappings": [],
            "source_vertex_mappings": [
                {"column": {"index": 0, "name": "source"}, "property": "id"}
            ],
            "destination_vertex_mappings": [
                {"column": {"index": 1, "name": "target"}, "property": "id"}
            ],
        },
    ],
}


@pytest.fixture(scope="module")
def interactive_driver():
    driver = Driver()
    yield driver
    driver.close()


@pytest.fixture(scope="module")
def interactive_session(interactive_driver):
    yield interactive_driver.session()


@pytest.fixture(scope="module")
def neo4j_session(interactive_driver):
    _neo4j_sess = interactive_driver.getNeo4jSession()
    yield _neo4j_sess
    _neo4j_sess.close()


@pytest.fixture(scope="function")
def create_modern_graph(interactive_session):
    create_graph_request = CreateGraphRequest.from_dict(modern_graph_full)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_modern_graph_multiple_edge_property(interactive_session):
    create_graph_request = CreateGraphRequest.from_dict(
        modern_graph_multiple_edge_properties
    )
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_graph_algo_graph(interactive_session):
    create_graph_request = CreateGraphRequest.from_dict(graph_algo_graph)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_modern_graph_with_temporal_type(interactive_session):
    modern_graph_temporal_type = CreateGraphRequest.from_dict(
        modern_graph_vertex_only_temporal
    )
    resp = interactive_session.create_graph(modern_graph_temporal_type)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_vertex_only_modern_graph(interactive_session):
    create_graph_request = CreateGraphRequest.from_dict(modern_graph_vertex_only)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_partial_modern_graph(interactive_session):
    create_graph_request = CreateGraphRequest.from_dict(modern_graph_partial)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_graph_with_custom_pk_name(interactive_session):
    modern_graph_custom_pk_name = copy.deepcopy(modern_graph_full)
    for vertex_type in modern_graph_custom_pk_name["schema"]["vertex_types"]:
        vertex_type["properties"][0]["property_name"] = "custom_id"
        vertex_type["primary_keys"] = ["custom_id"]
    create_graph_request = CreateGraphRequest.from_dict(modern_graph_custom_pk_name)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_graph_with_var_char_property(interactive_session):
    modern_graph_custom_pk_name = copy.deepcopy(modern_graph_full)
    for vertex_type in modern_graph_custom_pk_name["schema"]["vertex_types"]:
        # replace each string property with var_char
        for prop in vertex_type["properties"]:
            if prop["property_type"]:
                if "string" in prop["property_type"]:
                    prop["property_type"]["string"] = {"var_char": {"max_length": 2}}
    create_graph_request = CreateGraphRequest.from_dict(modern_graph_custom_pk_name)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


@pytest.fixture(scope="function")
def create_graph_algo_graph_with_x_csr_params(interactive_session):
    """
    Create a relative complex graph with small max_vertex_num and import
    relatively large data to test the bulk loading procedure could handle
    the case.
    """
    new_graph_algo_schema = new_graph_algo.copy()
    for vertex_type in new_graph_algo_schema["schema"]["vertex_types"]:
        vertex_type["x_csr_params"] = {"max_vertex_num": 1}
    create_graph_request = CreateGraphRequest.from_dict(new_graph_algo_schema)
    resp = interactive_session.create_graph(create_graph_request)
    assert resp.is_ok()
    graph_id = resp.get_value().graph_id
    yield graph_id
    delete_running_graph(interactive_session, graph_id)


def wait_job_finish(sess: Session, job_id: str):
    assert job_id is not None
    while True:
        resp = sess.get_job(job_id)
        assert resp.is_ok()
        status = resp.get_value().status
        print("job status: ", status)
        if status == "SUCCESS":
            return True
        elif status == "FAILED":
            return False
        else:
            time.sleep(1)


def import_data_to_vertex_only_modern_graph(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_vertex_only_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def import_data_to_modern_graph_temporal_type(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_vertex_only_import_config)
    # change person.csv to /tmp/person.csv
    schema_mapping.vertex_mappings[0].inputs[0] = "person.csv"
    schema_mapping.loading_config.data_source.location = "/tmp"
    print("schema_mapping: ", schema_mapping.to_dict())
    tmp_person_csv = open("/tmp/person.csv", "w")
    tmp_person_csv.write(
        "id|name|age|birthday\n"
        + "1|marko|29|628646400000\n"
        + "2|vadas|27|445910400000\n"
        + "4|josh|32|491788800000\n"
        + "6|peter|35|531273600000"
    )
    tmp_person_csv.close()
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def import_data_to_vertex_only_modern_graph_no_wait(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_vertex_only_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    print("job_id: ", job_id)


def import_data_to_partial_modern_graph(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_partial_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def import_data_to_full_modern_graph(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(modern_graph_full_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def import_long_string_data_data_to_vertex_only_modern_graph(
    sess: Session, graph_id: str
):
    schema_mapping = SchemaMapping.from_dict(modern_graph_vertex_only_import_config)
    # create a long string file under MODERN_GRAPH_DATA_DIR
    long_string_file = os.path.join(MODERN_GRAPH_DATA_DIR, "person_long_string.csv")

    # By default, the max length of a string property is 256,
    # a string with length 4096 should be enough to test the long string feature
    def generate_large_string(sample: str):
        return sample * 4096

    with open(long_string_file, "w") as f:
        f.write("id|name|age\n")
        f.write("1|" + generate_large_string("marko") + "|29\n")
        f.write("2|" + generate_large_string("vadas") + "|27\n")
        f.write("4|" + generate_large_string("josh") + "|32\n")
        f.write("6|" + generate_large_string("peter") + "|35\n")

    schema_mapping.vertex_mappings[0].inputs[0] = "person_long_string.csv"
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)
    # return a callable to clean up the long string file
    os.remove(long_string_file)


def import_data_to_full_graph_algo_graph(sess: Session, graph_id: str):
    schema_mapping = SchemaMapping.from_dict(graph_algo_graph_import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def import_data_to_new_graph_algo_graph(sess: Session, graph_id: str):
    # check whether GS_TEST_DIR is set
    if "GS_TEST_DIR" not in os.environ:
        raise Exception("GS_TEST_DIR is not set")

    GS_TEST_DIR = os.environ["GS_TEST_DIR"]
    NEW_GRAPH_ALGO_SOURCE_DIR = os.path.join(GS_TEST_DIR, "flex/new_graph_algo")
    import_config = copy.deepcopy(new_graph_algo_import_config)
    import_config["loading_config"]["data_source"][
        "location"
    ] = NEW_GRAPH_ALGO_SOURCE_DIR
    schema_mapping = SchemaMapping.from_dict(import_config)
    resp = sess.bulk_loading(graph_id, schema_mapping)
    assert resp.is_ok()
    job_id = resp.get_value().job_id
    assert wait_job_finish(sess, job_id)


def submit_query_via_neo4j_endpoint(
    neo4j_sess: Neo4jSession, graph_id: str, query: str
):
    result = neo4j_sess.run(query)
    # check have 1 records, result 0
    result_cnt = 0
    for record in result:
        print("record: ", record)
        result_cnt += 1
    print("result count: ", result_cnt, " for query ", query)


def run_cypher_test_suite(neo4j_sess: Neo4jSession, graph_id: str, queries: list[str]):
    for query in queries:
        submit_query_via_neo4j_endpoint(neo4j_sess, graph_id, query)


def call_procedure(neo4j_sess: Neo4jSession, graph_id: str, proc_name: str, *args):
    query = "CALL " + proc_name + "(" + ",".join([str(item) for item in args]) + ")"
    result = neo4j_sess.run(query)
    for record in result:
        print(record)


def delete_running_graph(sess: Session, graph_id: str):
    # restart the service on graph "1"
    print("delete running graph {}", graph_id)
    # first the the service status, to get the graph id
    service_status = sess.get_service_status()
    assert service_status.is_ok()
    running_graph_id = service_status.get_value().graph.id
    if running_graph_id is None:
        print("No running graph")
        return
    if running_graph_id != graph_id:
        print("The request graph is not running, safe to delete")
    else:
        resp = sess.start_service(StartServiceRequest(graph_id="1"))
        assert resp.is_ok()
    # drop the graph
    resp = sess.delete_graph(graph_id)


def create_procedure(
    sess: Session, graph_id: str, name: str, query: str, description="test proc"
):
    request = CreateProcedureRequest(
        name=name, description=description, type="cypher", query=query
    )

    resp = sess.create_procedure(graph_id, request)
    if not resp.is_ok():
        print("Failed to create procedure: ", resp.get_status_message())
        raise Exception(
            "Failed to create procedure, status: ", resp.get_status_message()
        )
    proc_id = resp.get_value().procedure_id
    return proc_id


def delete_procedure(sess: Session, graph_id: str, proc_id: str):
    resp = sess.delete_procedure(graph_id, proc_id)
    if not resp.is_ok():
        print("Failed to delete procedure: ", resp.get_status_message())
        raise Exception(
            "Failed to delete procedure, status: ", resp.get_status_message()
        )


def update_procedure(sess: Session, graph_id: str, proc_id: str, desc: str):
    request = UpdateProcedureRequest(description=desc)
    resp = sess.update_procedure(graph_id, proc_id, request)
    if not resp.is_ok():
        print("Failed to update procedure: ", resp.get_status_message())
        raise Exception(
            "Failed to update procedure, status: ", resp.get_status_message()
        )


def start_service_on_graph(interactive_session, graph_id: str):
    resp = interactive_session.start_service(StartServiceRequest(graph_id=graph_id))
    assert resp.is_ok()
    # wait three second to let compiler get the new graph
    time.sleep(3)


def ensure_compiler_schema_ready(
    interactive_session, neo4j_session: Neo4jSession, graph_id: str
):
    rel_graph_meta = interactive_session.get_graph_schema(graph_id).get_value()
    max_times = 10
    while True:
        if max_times == 0:
            raise Exception("compiler schema is not ready")
        res = neo4j_session.run("CALL gs.procedure.meta.schema();")
        val = res.single().value()
        compiler_graph_schema = GetGraphSchemaResponse.from_json(val)
        # print("compiler_graph_schema: ", compiler_graph_schema)
        # print("rel_graph_meta: ", rel_graph_meta)
        if compiler_graph_schema == rel_graph_meta:
            break
        print("compiler schema is not ready, wait for 1 second")
        time.sleep(1)
        max_times -= 1
    print("compiler schema is ready")


def send_get_request(url, timeout):
    try:
        import requests

        response = requests.get(url, timeout=timeout)  # noqa
    except requests.exceptions.Timeout:
        raise Exception("Got timeout exception when sending get request to ", url)
    except Exception as e:
        raise Exception(
            "Got exception when sending get request to ", url, " exception: ", e
        )


def send_get_request_periodically(url, timeout, interval, times):
    for i in range(times):
        send_get_request(url, timeout)
        time.sleep(interval)

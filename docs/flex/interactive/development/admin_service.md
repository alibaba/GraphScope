# Interactive Admin Service


If you are just a user trying to use Interactive, you can ignore the content of this section. However, if you are a developer and curious about managing the Interactive service at runtime, this article will guide you through the RESTful HTTP interfaces exposed by the Interactive Admin service.

## API Overview

| Category            | API name        | Method | URL                                   | Explanation                                                        |
|---------------------|-----------------|--------|---------------------------------------|--------------------------------------------------------------------|
| GraphManagement     | ListGraphs      | GET    | /v1/graph                             | Get all graphs in current interactive service, the schema for each graph is returned. |
| GraphManagement     | GetGraphSchema  | GET    | v1/graph/{graph_name}/schema          | Get the schema for the specified graph.                            |
| GraphManagement     | CreateGraph     | POST   | /v1/graph                             | Create an empty graph with the specified schema.                    |
| GraphManagement     | DeleteGraph     | DELETE | v1/graph/{graph_name}                 | Delete the specified graph.                                        |
| GraphManagement     | ImportGraph     | POST   | v1/graph/{graph_name}/dataloading     | Import data to graph.                                              |
| ProcedureManagement | CreateProcedure | POST   | /v1/graph/{graph_name}/procedure      | Create a new stored procedure bound to a graph.                    |
| ProcedureManagement | ShowProcedures  | GET    | /v1/graph/{graph_name}/procedure      | Get all procedures bound to the specified graph.                   |
| ProcedureManagement | GetProcedure    | GET    | v1/graph/{graph_name}/procedure/{procedure_name} | Get the metadata of the procedure.                                 |
| ProcedureManagement | DeleteProcedure | DELETE | /v1/graph/{graph_name}/procedure/{procedure_name} | Delete the specified procedure.                                   |
| ProcedureManagement | UpdateProcedure | PUT    | /v1/graph/{graph_name}/procedure/{procedure_name} | Update some metadata for the specified procedure, i.e. update description, enable/disable. |
| ServiceManagement   | StartService    | POST   | /v1/service/start                     | Start the service on the graph specified in request body.          |
| ServiceManagement   | ServiceStatus   | GET    | /v1/service/status                    | Get current service status.                                        |
| NodeMetrics         | NodeStatus      | GET    | /v1/node/status                       | Get the metrics of current node.                                   |

## Configuration

You can configure the listening port and other properties for admin service, for details, please check [Configuration](../configuration)

## GraphManagment
### ListGraphs
#### Request format

```http
GET /v1/graph/ HTTP/1.1
Content-Type: text/plain
```
#### Request body
```json
```

#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```

#### Response body
```json
[
  {
    "name": "modern_graph",
    "schema": {
      "vertex_types": [
        {
          "type_id": "0",
          "type_name": "person",
          "properties": [
            {
              "property_id": "0",
              "property_name": "id",
              "property_type": {
                "primitive_type": "DT_SIGNED_INT64"
              }
            },
            {
              "property_id": "1",
              "property_name": "name",
              "property_type": {
                "primitive_type": "DT_STRING"
              }
            },
            {
              "property_id": "2",
              "property_name": "age",
              "property_type": {
                "primitive_type": "DT_SIGNED_INT32"
              }
            }
          ],
          "primary_keys": [
            "id"
          ]
        },
        {
          "type_id": "1",
          "type_name": "software",
          "properties": [
            {
              "property_id": "0",
              "property_name": "id",
              "property_type": {
                "primitive_type": "DT_SIGNED_INT64"
              }
            },
            {
              "property_id": "1",
              "property_name": "name",
              "property_type": {
                "primitive_type": "DT_STRING"
              }
            },
            {
              "property_id": "2",
              "property_name": "lang",
              "property_type": {
                "primitive_type": "DT_STRING"
              }
            }
          ],
          "primary_keys": [
            "id"
          ]
        }
      ],
      "edge_types": [
        {
          "type_id": "0",
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
              "property_id": "0",
              "property_name": "weight",
              "property_type": {
                "primitive_type": "DT_DOUBLE"
              }
            }
          ]
        },
        {
          "type_id": "1",
          "type_name": "created",
          "vertex_type_pair_relations": [
            {
              "source_vertex": "person",
              "destination_vertex": "software",
              "relation": "ONE_TO_MANY",
            }
          ],
          "properties": [
            {
              "property_id": "0",
              "property_name": "weight",
              "property_type": {
                "primitive_type": "DT_DOUBLE"
              }
            }
          ]
        }
      ]
    }
  }
]
```

### CreateGraph

#### Request format

```http
POST /v1/graph/ HTTP/1.1
Content-Type: application/json
```

#### Request body
```json
{
    "name": "modern",
    "schema": {
        "vertex_types": [
            {
                "type_id": 0,
                "type_name": "person",
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "id",
                        "property_type": {
                            "primitive_type": "DT_SIGNED_INT64"
                        }
                    },
                    {
                        "property_id": 1,
                        "property_name": "name",
                        "property_type": {
                            "primitive_type": "DT_STRING"
                        }
                    },
                    {
                        "property_id": 2,
                        "property_name": "age",
                        "property_type": {
                            "primitive_type": "DT_SIGNED_INT32"
                        }
                    }
                ],
                "primary_keys": [
                    "id"
                ]
            },
            {
                "type_id": 1,
                "type_name": "software",
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "id",
                        "property_type": {
                            "primitive_type": "DT_SIGNED_INT64"
                        }
                    },
                    {
                        "property_id": 1,
                        "property_name": "name",
                        "property_type": {
                            "primitive_type": "DT_STRING"
                        }
                    },
                    {
                        "property_id": 2,
                        "property_name": "lang",
                        "property_type": {
                            "primitive_type": "DT_STRING"
                        }
                    }
                ],
                "primary_keys": [
                    "id"
                ]
            }
        ],
        "edge_types": [
            {
                "type_id": 0,
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
                        "property_id": 0,
                        "property_name": "weight",
                        "property_type": {
                            "primitive_type": "DT_DOUBLE"
                        }
                    }
                ]
            },
            {
                "type_id": 1,
                "type_name": "created",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "software",
                        "relation": "ONE_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "weight",
                        "property_type": {
                            "primitive_type": "DT_DOUBLE"
                        }
                    }
                ]
            }
        ]
    }
}
```
#### Response format

```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body

```json
{
  "code": 0,
  "message": "message"
}
```

### DeleteGraph
#### Request format
```http
DELETE /v1/graph/{graph_name} HTTP/1.1
Content-Type: text/plain
```
#### Request body
Empty request body
#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "code": 0,
  "message": "message"
}
```

### GetGraphSchema
#### Request format
```http
GET /v1/graph/{graph_name} HTTP/1.1
Content-Type: text/plain
```
#### Request body

Empty request body.

#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
    "name": "modern",
    "schema": {
        "vertex_types": [
            {
                "type_id": 0,
                "type_name": "person",
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "id",
                        "property_type": {
                            "primitive_type": "DT_SIGNED_INT64"
                        }
                    },
                    {
                        "property_id": 1,
                        "property_name": "name",
                        "property_type": {
                            "primitive_type": "DT_STRING"
                        }
                    },
                    {
                        "property_id": 2,
                        "property_name": "age",
                        "property_type": {
                            "primitive_type": "DT_SIGNED_INT32"
                        }
                    }
                ],
                "primary_keys": [
                    "id"
                ]
            },
            {
                "type_id": 1,
                "type_name": "software",
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "id",
                        "property_type": {
                            "primitive_type": "DT_SIGNED_INT64"
                        }
                    },
                    {
                        "property_id": 1,
                        "property_name": "name",
                        "property_type": {
                            "primitive_type": "DT_STRING"
                        }
                    },
                    {
                        "property_id": 2,
                        "property_name": "lang",
                        "property_type": {
                            "primitive_type": "DT_STRING"
                        }
                    }
                ],
                "primary_keys": [
                    "id"
                ]
            }
        ],
        "edge_types": [
            {
                "type_id": 0,
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
                        "property_id": 0,
                        "property_name": "weight",
                        "property_type": {
                            "primitive_type": "DT_DOUBLE"
                        }
                    }
                ]
            },
            {
                "type_id": 1,
                "type_name": "created",
                "vertex_type_pair_relations": [
                    {
                        "source_vertex": "person",
                        "destination_vertex": "software",
                        "relation": "ONE_TO_MANY",
                    }
                ],
                "properties": [
                    {
                        "property_id": 0,
                        "property_name": "weight",
                        "property_type": {
                            "primitive_type": "DT_DOUBLE"
                        }
                    }
                ]
            }
        ]
    }
}
```

### ImportGraph
#### Request format
```http
POST /v1/graph/{graph_name}/dataloading HTTP/1.1
Content-Type: application/json
```
#### Request body
```json
{
  "graph": "graph",
  "loading_config": {
    "data_source": {
      "scheme": "file"
	  "location": {path_to_file},
    },
    "format": {
      "metadata": {
        "key": "metadata"
      },
      "type": "type"
    },
    "import_option": "init"
  },
  "edge_mappings": [
    {
      "column_mappings": [
        {
          "column": {
            "index": 0,
            "name": "name"
          },
          "property": "property"
        },
        {
          "column": {
            "index": 0,
            "name": "name"
          },
          "property": "property"
        }
      ],
      "destination_vertex_mappings": [
        {
          "column": {
            "index": 0,
            "name": "name"
          }
        },
        {
          "column": {
            "index": 0,
            "name": "name"
          }
        }
      ],
      "inputs": [
        "inputs",
        "inputs"
      ],
      "source_vertex_mappings": [
        {
          "column": {
            "index": 0,
            "name": "name"
          }
        },
        {
          "column": {
            "index": 0,
            "name": "name"
          }
        }
      ],
      "type_triplet": {
        "destination_vertex": "destination_vertex",
        "edge": "edge",
        "source_vertex": "source_vertex"
      }
    },
    {
      "column_mappings": [
        {
          "column": {
            "index": 0,
            "name": "name"
          },
          "property": "property"
        },
        {
          "column": {
            "index": 0,
            "name": "name"
          },
          "property": "property"
        }
      ],
      "destination_vertex_mappings": [
        {
          "column": {
            "index": 0,
            "name": "name"
          }
        },
        {
          "column": {
            "index": 0,
            "name": "name"
          }
        }
      ],
      "inputs": [
        "inputs",
        "inputs"
      ],
      "source_vertex_mappings": [
        {
          "column": {
            "index": 0,
            "name": "name"
          }
        },
        {
          "column": {
            "index": 0,
            "name": "name"
          }
        }
      ],
      "type_triplet": {
        "destination_vertex": "destination_vertex",
        "edge": "edge",
        "source_vertex": "source_vertex"
      }
    }
  ],
  "vertex_mappings": [
    {
      "column_mappings": [
        {
          "column": {
            "index": 0,
            "name": "name"
          },
          "property": "property"
        },
        {
          "column": {
            "index": 0,
            "name": "name"
          },
          "property": "property"
        }
      ],
      "inputs": [
        "inputs",
        "inputs"
      ],
      "type_name": "type_name"
    },
    {
      "column_mappings": [
        {
          "column": {
            "index": 0,
            "name": "name"
          },
          "property": "property"
        },
        {
          "column": {
            "index": 0,
            "name": "name"
          },
          "property": "property"
        }
      ],
      "inputs": [
        "inputs",
        "inputs"
      ],
      "type_name": "type_name"
    }
  ]
}
```
#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body

```json
{
  "code": 0,
  "message": "message"
}
```

## ProcedureManagement

### CreateProcedure
#### Request format
```http
POST /v1/graph/{graph_name}/procedure HTTP/1.1
Content-Type: application/json
```
#### Request body
```json
{
  "bound_graph": "bound_graph",
  "description": "description",
  "enable": true,
  "name": "name",
  "query" : "MATCH(n) return COUNT(n);",
  "type": "cypher"
}
```
#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "code": 0,
  "message": "message"
}
```

### ListAllProcedure
#### Request format
```http
GET /v1/graph/{graph_name}/procedure HTTP/1.1
Content-Type: application/json
```
#### Request body
Empty Request body.
#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "code": 0,
  "message": "message"
}
```
### GetProcedure
#### Request format
```http
GET /v1/graph/{graph_name}/procedure/{procedure_name} HTTP/1.1
Content-Type: application/json
```
#### Request body
Empty Request body.
#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "code": 0,
  "message": "message"
}
```
### UpdateProcedure
#### Request format
```http
PUT /v1/graph/{graph_name}/procedure/{procedure_name} HTTP/1.1
Content-Type: application/json
```
#### Request body
```json
{
  "description": "description",
  "enable": true,
}
```
#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "code": 0,
  "message": "message"
}
```
### DeleteProcedure
#### Request format
```http
DELETE /v1/graph/{graph_name}/procedure/{procedure_name} HTTP/1.1
Content-Type: application/json
```
#### Request body
Empty Request body.

#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "code": 0,
  "message": "message"
}
```

## ServiceManagement
### StartService
#### Request format
```http
POST /v1/service/start HTTP/1.1
Content-Type: application/json
```
#### Request body
```json
{
	"graph_name": "modern_graph"
}
```

#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "code": 0,
  "message": "message"
}
```

### ServiceStatus
#### Request format
```http
GET /v1/service/status HTTP/1.1
Content-Type: application/json
```
#### Request body
Empty request body.

#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "graph_name": "graph_name",
  "query_port": 6,
  "status": "running"
}
```

## NodeMetrics
### Nodestatus
#### Request format
```http
GET /v1/node/status HTTP/1.1
Content-Type: text/plain
```
#### Request body
Empty request body.

#### Response format
```http
HTTP/1.1 200 Accepted
Content-type: application/json
```
#### Response body
```json
{
  "cpu_usage": "0.2",
  "memory_usage": "0.3"
}
```
# GraphScope Interactive Admin Service Documentation

## Introduction

Welcome to the GraphScope Interactive Admin Service documentation. This guide is tailored for developers and administrators seeking to manage the Interactive service more efficiently. Here, we delve into the intricate workings of the RESTful HTTP interfaces provided by the Interactive Admin service, offering a comprehensive toolkit for real-time service management. This document is crucial for those looking to customize or enhance their GraphScope Interactive experience.

## API Overview

The table below provides an overview of the available APIs:


| API name        | Method and URL                        | Explanation                                                        |
|-----------------|---------------------------------------|--------------------------------------------------------------------|
| ListGraphs      | GET /v1/graph                             | Get all graphs in current interactive service, the schema for each graph is returned. |
| GetGraphSchema  | GET /v1/graph/{graph}/schema          | Get the schema for the specified graph.                            |
| CreateGraph     | POST /v1/graph                             | Create an empty graph with the specified schema.                    |
| DeleteGraph     | DELETE /v1/graph/{graph}                 | Delete the specified graph.                                        |
| ImportGraph     | POST /v1/graph/{graph}/dataloading     | Import data to graph.                                              |
| CreateProcedure | POST /v1/graph/{graph}/procedure      | Create a new stored procedure bound to a graph.                    |
| ShowProcedures  | GET /v1/graph/{graph}/procedure      | Get all procedures bound to the specified graph.                   |
| GetProcedure    | GET /v1/graph/{graph}/procedure/{proc_name} | Get the metadata of the procedure.                                 |
| DeleteProcedure | DELETE /v1/graph/{graph}/procedure/{proc_name} | Delete the specified procedure.                                   |
| UpdateProcedure | PUT /v1/graph/{graph}/procedure/{proc_name} | Update some metadata for the specified procedure, i.e. update description, enable/disable. |
| StartService   | POST /v1/service/start                     | Start the service on the graph specified in request body.          |
| ServiceStatus  | GET /v1/service/status                    | Get current service status.                                        |
| SystemMetrics     | GET /v1/node/status                       | Get the system metrics of current host/pod, i.e. CPU usage, memory usages.                    |


## Detailed API Documentation

For each API, the documentation will include a detailed description, request format, example curl command, expected response format and body, and status codes. Here's an example for one of the APIs:


### ListGraphs API (GraphManagement Category)

#### Description
This API lists all graphs currently managed by the Interactive service, providing detailed schema information for each.

#### HTTP Request
- **Method**: GET
- **Endpoint**: `/v1/graph`
- **Content-type**: `application/json`

#### Curl Command Example
```bash
curl -X GET -H "Content-Type: application/json" "http://[host]/v1/graph"
```

#### Expected Response
- **Format**: `application/json`
- **Body**:
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

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.

### CreateGraph (GraphManagement Category)

#### Description
This API create a new graph according to the specified schema in request body.


#### HTTP Request
- **Method**: POST
- **Endpoint**: `/v1/graph`
- **Content-type**: `application/json`
- **Body**: 
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

#### Curl Command Example
```bash
curl -X POST  -H "Content-Type: application/json" -d @path/to/yourfile.json  "http://[host]/v1/graph"
```

#### Expected Response

- **Format**: `application/json`
- **Body**:
```json
{
  "message": "message"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.
- `404 Not Found`: Graph not found

### DeleteGraph  (GraphManagement Category)

#### Description
Delete a graph by name, including schema, indices and stored procedures.

#### HTTP Request
- **Method**: DELETE
- **Endpoint**: `/v1/graph/{graph_name}`
- **Content-type**: `application/json`


#### Curl Command Example
```bash
curl -X DELETE  -H "Content-Type: application/json" "http://[host]/v1/graph/{graph_name}"
```

#### Expected Response
- **Format**: application/json
- **Body**:
```json
{
  "message": "message"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.

### GetGraphSchema  (GraphManagement Category)

#### Description
Get the schema for the specified graph.

#### HTTP Request
- **Method**: GET
- **Endpoint**: `/v1/graph/{graph_name}`
- **Content-type**: `application/json`

#### Curl Command Example
```bash
curl -X GET  -H "Content-Type: application/json" "http://[host]/v1/graph/{graph_name}"
```


#### Expected Response
- **Format**: `application/json`
- **Body**:
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

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.
- `404 Not Found`: Graph not found

### ImportGraph  (GraphManagement Category)

#### Description

Import data to empty graph.

#### HTTP Request

- **Method**: POST
- **Endpoint**: `/v1/graph/{graph_name}/dataloading`
- **Content-type**: `application/json`
- **Body**:
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

#### Curl Command Example
```bash
curl -X POST -H "Content-Type: application/json" -d @path/to/json "http://[host]/v1/graph/{graph_name}/dataloading"
```

#### Expected Response
- **Format**: `application/json`
- **Body**:
```json
{
  "message": "message"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.
- `404 Not Found`: Graph not found.


### CreateProcedure (ProcedureManagement Category)

#### Description 

Create a new stored procedure.

#### HTTP Request
- **Method**: POST
- **Endpoint**: `/v1/graph/{graph_name}/procedure`
- **Content-type**: `application/json`
- **Body**:
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

#### Curl Command Example
```bash
curl -X POST -H "Content-Type: application/json" -d @/path/to/json "http://[host]/v1/graph/{graph_name}/procedure"
```


#### Expected Response
- **Format**: application/json
- **Body**:
```json
{
  "message": "message"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.
- `404 Not FOund`: Graph not found.

### ListAllProcedure  (ProcedureManagement Category)

#### Description

List all procedures bound to a graph.

#### HTTP Request
- **Method**: GET
- **Endpoint**: `/v1/graph/{graph_name}/procedure`
- **Content-type**: `application/json`

#### Curl Command Example
```bash
curl -X GET -H "Content-Type: application/json" "http://[host]/v1/graph/{graph_name}/procedure"
```

#### Expected Response
- **Format**: application/json
- **Body**:
```json
[
  {
    "bound_graph": "bound_graph",
    "description": "description",
    "enable": true,
    "name": "name",
    "params": [
      {
        "name": "name",
        "type": "type"
      },
      {
        "name": "name",
        "type": "type"
      }
    ],
    "query": "query",
    "returns": [
      {
        "name": "name",
        "type": "type"
      },
      {
        "name": "name",
        "type": "type"
      }
    ],
    "type": "cpp"
  }
]
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.
- `404 Not Found`: Graph not found


### GetProcedure (ProcedureManagement Category)

#### Description

Get a single procedure's metadata.

#### HTTP Request
- **Method**: GET
- **Endpoint**: `/v1/graph/{graph_name}/procedure/{procedure_name}`
- **Content-type**: `application/json`

#### Curl Command Example
```bash
curl -X GET  -H "Content-Type: application/json" "http://[host]/v1/graph/{graph_name}/procedure/{procedure_name}"
```


#### Expected Response
- **Format**: application/json
- **Body**:
```json

{
  "bound_graph": "bound_graph",
  "description": "description",
  "enable": true,
  "name": "name",
  "params": [
    {
      "name": "name",
      "type": "type"
    },
    {
      "name": "name",
      "type": "type"
    }
  ],
  "query": "query",
  "returns": [
    {
      "name": "name",
      "type": "type"
    },
    {
      "name": "name",
      "type": "type"
    }
  ],
  "type": "cpp"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.


### UpdateProcedure (ProcedureManagement Category)

#### Description

Update a procedure's metadata, enable/disable status, description. The procedure's name cannot be modified.


#### HTTP Request
- **Method**: PUT
- **Endpoint**: `/v1/graph/{graph_name}/procedure/{procedure_name}`
- **Content-type**: `application/json`
- **Body**:
```json
{
  "description": "description",
  "enable": true,
}
```

#### Curl Command Example
```bash
curl -X PUT  -H "Content-Type: application/json" -d @/path/to/json "http://[host]//v1/graph/{graph_name}/procedure/{procedure_name}"
```

#### Expected Response
- **Format**: `application/json`
- **Body**:
```json
{
  "bound_graph": "bound_graph",
  "description": "description",
  "enable": true,
  "name": "name",
  "params": [
    {
      "name": "name",
      "type": "type"
    },
    {
      "name": "name",
      "type": "type"
    }
  ],
  "query": "query",
  "returns": [
    {
      "name": "name",
      "type": "type"
    },
    {
      "name": "name",
      "type": "type"
    }
  ],
  "type": "cpp"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.
- `404 Not Found`: Graph or procedure not found.

### DeleteProcedure (ProcedureManagement Category)

#### Description

Delete a procedure bound to the graph.

#### HTTP Request

- **Method**: DELETE
- **Endpoint**: `/v1/graph/{graph_name}/procedure/{procedure_name}`
- **Content-type**: `application/json`


#### Curl Command Example
```bash
curl -X DELETE -H "Content-Type: application/json" "http://[host]/v1/graph/{graph_name}/procedure/{procedure_name}"
```

#### Expected Response
- **Format**: application/json
- **Body**:
```json
{
  "message": "message"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.
- `404 Not Found`: Graph or procedure not found.

### StartService (ServiceManagement Category)

#### Description 

Start the query service on a graph. The `graph_name` param can be empty, indicating restarting on current running graph.

1. After the AdminService receives this request, the current actor scope for query actors will be cancelled.
2. During the scope cancellation process of the query actors or after scope cancellation is completed, all requests sent to the query_service will fail and be rejected. 
The response of the http request will be like
```json
{
  "code": 500,
  "message" : "Unable to send message, the target actor has been canceled!"
}
```
3. After the previous graph is closed and new graph is opened, the new query actors will be available in a new scope. 
4. The query service is now ready to serve requests on the new graph.
 

#### HTTP Request
- **Method**: POST
- **Endpoint**: `/v1/service/start`
- **Content-type**: `application/json`
- **Body**:
```json
{
	"graph_name": "modern_graph"
}
```

#### Curl Command Example
```bash
curl -X POST -H "Content-Type: application/json" "http://[host]/v1/service/start"
```

#### Expected Response
- **Format**: application/json
- **Body**:
```json
{
  "message": "message"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.

### ServiceStatus

#### Description

Get the status of current service.

#### HTTP Request
- **Method**: GET
- **Endpoint**: `/v1/service/status`
- **Content-type**: `application/json`

#### Expected Response
- **Format**: application/json
- **Body**:
```json
{
  "graph_name": "graph_name",
  "query_port": 6,
  "status": "running"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.


### SystemMetrics (NodeMetrics Category)

#### Description

Get node status.

#### HTTP Request
- **Method**: GET
- **Endpoint**: `/v1/node/status`
- **Content-type**: `application/json`

#### Curl Command Example
```bash
curl -X GET  -H "Content-Type: application/json" "http://[host]/v1/node/status"
```

#### Expected Response
- **Format**: application/json
- **Body**:
```json
{
  "cpu_usage": "0.2",
  "memory_usage": "0.3"
}
```

#### Status Codes
- `200 OK`: Request successful.
- `500 Internal Error`: Server internal Error.
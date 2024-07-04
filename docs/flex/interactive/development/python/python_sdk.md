# Python SDK Reference

The Interactive Python SDK Reference is a comprehensive guide designed to assist developers in integrating the Interactive service into their Python applications. This SDK allows users to seamlessly connect to Interactive and harness its powerful features for graph management, stored procedure management, and query execution.

## Requirements.

Python 3.7+

## Installation & Usage
### pip install

```bash
pip3 install gs_interactive
```

Then import the package:
```python
import gs_interactive
```

### Setuptools

Install via [Setuptools](http://pypi.python.org/pypi/setuptools).

```sh
python3 setup.py build_proto
python setup.py install --user
```
(or `sudo python3 setup.py install` to install the package for all users)

Then import the package:
```python
import gs_interactive
```

### Tests

Execute `pytest` to run the tests.

## Getting Started

First, install and start the interactive service via [Interactive Getting Started](https://graphscope.io/docs/flex/interactive/getting_started), and you will get the all the endpoints for the Interactive service.

```bash
You can connect to Interactive service with Interactive SDK, with following environment variables declared.

############################################################################################
    export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:{admin_port}
    export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:{storedproc_port}
    export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:{cypher_port}
    export INTERACTIVE_GREMLIN_ENDPOINT=ws://127.0.0.1:{gremlin_port}/gremlin
############################################################################################
```

Remember to export these environment variables.

### Connect and submit a query

Interactive provide you with a default graph, `modern_graph`. You can connect to the interactive endpoint, and try to run a simple query with following code.

```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
with driver.getNeo4jSession() as session:
    result = session.run("MATCH(n) RETURN COUNT(n);")
    for record in result:
        print(record)
```

### Create a new graph

To create a new graph, user need to specify the name, description, vertex types and edges types.
For the detail data model of the graph, please refer to [Data Model](../../data_model). 

In this example, we will create a simple graph with only one vertex type `persson`, and one edge type named `knows`.

```python
# Graph schema is defined by a json string.
test_graph_def = {
    "name": "test_graph",
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

# get the interactive session
sess = driver.session()

# construct the request from json string
create_graph_request = CreateGraphRequest.from_dict(test_graph_def)
resp = sess.create_graph(create_graph_request)
assert resp.is_ok()
graph_id = resp.get_value().graph_id
print("Graph id: ", graph_id)
```

In the above example, a graph with name `test_graph` is defined via a json string. You can also define the graph with programmatic interface provided by [CreateGraphRequest](./CreateGraphRequest.md). So you call the method `createGraph`, a string reprensents the unique identifier of the graph is returned.


### Import data to the graph

After a new graph is created, you may want to import data into the newly created graph. 
For the detail configuration of data import, please refer to [Data Import Configuration](../../data_import).

For example, you can import the local csv files into the `test_graph`. Note that, currently only csv files are supported now. Remember to replase `/path/to/person.csv` and `/path/to/person_knows_person.csv` with the actual local path. You can download them from [GraphScope Interactive Github reop](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/modern_graph).

```python
test_graph_datasource = {
     "loading_config":{
        "data_source":{
            "scheme": "file"
        },
        "import_option": "init",
        "format":{
            "type": "csv"
        },
     },
    "vertex_mappings": [
        {
            "type_name": "person",
            "inputs": ["@/workspaces/GraphScope/flex/interactive/examples/modern_graph/person.csv"],
            "column_mappings": [
                {"column": {"index": 0, "name": "id"}, "property": "id"},
                {"column": {"index": 1, "name": "name"}, "property": "name"},
                {"column": {"index": 2, "name": "age"}, "property": "age"},
            ],
        }
    ],
    "edge_mappings": [
        {
            "type_triplet": {
                "edge": "knows",
                "source_vertex": "person",
                "destination_vertex": "person",
            },
            "inputs": [
                "@/workspaces/GraphScope/flex/interactive/examples/modern_graph/person_knows_person.csv"
            ],
            "source_vertex_mappings": [
                {"column": {"index": 0, "name": "person.id"}, "property": "id"}
            ],
            "destination_vertex_mappings": [
                {"column": {"index": 1, "name": "person.id"}, "property": "id"}
            ],
            "column_mappings": [
                {"column": {"index": 2, "name": "weight"}, "property": "weight"}
            ],
        }
    ],
}

bulk_load_request = SchemaMapping.from_dict(test_graph_datasource)
resp = sess.bulk_loading(graph_id, bulk_load_request)
assert resp.is_ok()
job_id = resp.get_value().job_id
print('The bulkloading job id is ', job_id)

# wait until the job has completed successfully.
while True:
    resp = sess.get_job(job_id)
    assert resp.is_ok()
    status = resp.get_value().status
    print("job status: ", status)
    if status == "SUCCESS":
        break
    elif status == "FAILED":
        raise Exception("job failed")
    else:
        time.sleep(1)
```

For each vertex/edge types, you need to provide the input data source and column mapping infomation.
Remember to add `@` at the begining of the local file path. 
`Session.bulkLoading()` will submit an dataloading job to the service, and we can query the status of the job via `Session.getJobStatus()`, and wait until the job has compleleted successfully.

### Create a stored procedure

Stored procedures can be registered into GraphScope Interactive to encapsulate and reuse complex graph operations. Interactive support both `cypher` and `c++` queries as stored procedures. 
With the following code, you will create a procedure named `testProcedure` which is definied via a `cypher` query.

```python
proc_name="test_procedure"
create_proc_request = CreateProcedureRequest(
    name=proc_name,
    description="test procedure",
    query="MATCH (n) RETURN COUNT(n);",
    type="cypher",
)
resp = sess.create_procedure(graph_id, create_proc_request)
assert resp.is_ok()
print("successfully create procedure: ", proc_name)
```

The procedure could not be invokded now, since currently interactive service has not been switched to the newly created `modern_graph`. We need to start the service on `modern_graph`.

### Start the query service on the new graph

Although Interactive supports multiple graphs in terms of logic and storage, it can only serve on one graph at a time. This means that at any given moment, only one graph is available to provide query service. So we need to switch to the newly created `modern_graph` with following code.

```python
resp = sess.start_service(
    start_service_request=StartServiceRequest(graph_id=graph_id)
)
assert resp.is_ok()
print("start service ok", resp)
```

### Submit queries to the new graph

After starting query service on the new graph, we are now able to submit queries to `modern_graph`.

#### Submit gremlin queries

````{note}
By default, the Gremlin service is disabled. To enable it, try specifying the Gremlin port when creating an interactive instance.


```bash
gsctl instance deploy --type interactive --gremlin-port 8183**
```

````

```python
query = "g.V().count();"
ret = []
gremlin_client = driver.getGremlinClient()
q = gremlin_client.submit(query)
while True:
    try:
        ret.extend(q.next())
    except StopIteration:
        break
print(ret)
```

#### Submit cypher queries

```python
```

#### Query the stored procedure

```python
query = "MATCH (n) RETURN COUNT(n);"
with driver.getNeo4jSession() as session:
    resp = session.run(query)
    for record in resp:
        print(record)
```

### Delete the graph

Finally, we can delete the graph. The graph data and stored procedure bound to the graph will also be deleted.

```python
resp = self._sess.delete_graph(graph_id)
assert resp.is_ok()
print("delete graph res: ", resp)
```

For the full example, please refer to [Java SDK Example](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/sdk/examples/java/interactive-example)

## Documentation for Service APIs

The APIs in interactive SDK are divided into five categories.
- GraphManagementApi
- ProcedureManagementApi
- JobManagementApi
- ServiceManagementApi
- QueryServiceApi

All URIs are relative to `${INTERACTIVE_ADMIN_ENDPOINT}`

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*GraphManagementApi* | [**BulkLoading**](./GraphManagementApi.md#Bulkloading) | **POST** /v1/graph/{graph_id}/dataloading | 
*GraphManagementApi* | [**CreateGraph**](./GraphManagementApi.md#CreateGraph) | **POST** /v1/graph | 
*GraphManagementApi* | [**DeleteGraph**](./GraphManagementApi.md#DeleteGraph) | **DELETE** /v1/graph/{graph_id} | 
*GraphManagementApi* | [**GetGraphMeta**](./GraphManagementApi.md#GetGraphMeta) | **GET** /v1/graph/{graph_id} | 
*GraphManagementApi* | [**GetGraphSchema**](./GraphManagementApi.md#GetGraphSchema) | **GET** /v1/graph/{graph_id}/schema | 
*GraphManagementApi* | [**ListGraphs**](./GraphManagementApi.md#ListGraphs) | **GET** /v1/graph | 
*GraphManagementApi* | [**GetGraphStatistics**](./GraphManagementApi.md#GetGraphStatistics) | **GET** /v1/graph/{graph_id}/statistics | 
*JobManagementApi* | [**CancelJob**](./JobManagementApi.md#CancelJob) | **DELETE** /v1/job/{job_id} | 
*JobManagementApi* | [**GetJobById**](./JobManagementApi.md#GetJobById) | **GET** /v1/job/{job_id} | 
*JobManagementApi* | [**ListJobs**](./JobManagementApi.md#ListJobs) | **GET** /v1/job | 
*ProcedureManagementApi* | [**CreateProcedure**](./ProcedureManagementApi.md#CreateProcedure) | **POST** /v1/graph/{graph_id}/procedure | 
*ProcedureManagementApi* | [**DeleteProcedure**](./ProcedureManagementApi.md#DeleteProcedure) | **DELETE** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*ProcedureManagementApi* | [**GetProcedure**](./ProcedureManagementApi.md#GetProcedure) | **GET** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*ProcedureManagementApi* | [**ListProcedures**](./ProcedureManagementApi.md#ListProcedures) | **GET** /v1/graph/{graph_id}/procedure | 
*ProcedureManagementApi* | [**UpdateProcedure**](./ProcedureManagementApi.md#UpdateProcedure) | **PUT** /v1/graph/{graph_id}/procedure/{procedure_id} | 
*ServiceManagementApi* | [**GetServiceStatus**](./ServiceManagementApi.md#GetServiceStatus) | **GET** /v1/service/status | 
*ServiceManagementApi* | [**RestartService**](./ServiceManagementApi.md#RestartService) | **POST** /v1/service/restart | 
*ServiceManagementApi* | [**StartService**](./ServiceManagementApi.md#StartService) | **POST** /v1/service/start | 
*ServiceManagementApi* | [**StopService**](./ServiceManagementApi.md#StopService) | **POST** /v1/service/stop | 
*QueryServiceApi* | [**CallProcedure**](./QueryServiceApi.md#CallProcedure) | **POST** /v1/graph/{graph_id}/query | 
*QueryServiceApi* | [**CallProcedureOnCurrentGraph**](./QueryServiceApi.md#CallProcedureOnCurrentGraph) | **POST** /v1/graph/current/query | 
<!-- TODO(zhanglei): Add Vertex/Edge APIs after supported by Interactive -->


## Documentation for Data Structures

 - [BaseEdgeType](./BaseEdgeType.md)
 - [BaseEdgeTypeVertexTypePairRelationsInner](./BaseEdgeTypeVertexTypePairRelationsInner.md)
 - [BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams](./BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams.md)
 - [BasePropertyMeta](./BasePropertyMeta.md)
 - [BaseVertexType](./BaseVertexType.md)
 - [BaseVertexTypeXCsrParams](./BaseVertexTypeXCsrParams.md)
 - [Collection](./Collection.md)
 - [CollectiveResults](./CollectiveResults.md)
 - [Column](./Column.md)
 - [ColumnMapping](./ColumnMapping.md)
 - [CreateEdgeType](./CreateEdgeType.md)
 - [CreateGraphRequest](./CreateGraphRequest.md)
 - [CreateGraphResponse](./CreateGraphResponse.md)
 - [CreateGraphSchemaRequest](./CreateGraphSchemaRequest.md)
 - [CreateProcedureRequest](./CreateProcedureRequest.md)
 - [CreateProcedureResponse](./CreateProcedureResponse.md)
 - [CreatePropertyMeta](./CreatePropertyMeta.md)
 - [CreateVertexType](./CreateVertexType.md)
 - [EdgeData](./EdgeData.md)
 - [EdgeMapping](./EdgeMapping.md)
 - [EdgeMappingDestinationVertexMappingsInner](./EdgeMappingDestinationVertexMappingsInner.md)
 - [EdgeMappingSourceVertexMappingsInner](./EdgeMappingSourceVertexMappingsInner.md)
 - [EdgeMappingSourceVertexMappingsInnerColumn](./EdgeMappingSourceVertexMappingsInnerColumn.md)
 - [EdgeMappingTypeTriplet](./EdgeMappingTypeTriplet.md)
 - [EdgeRequest](./EdgeRequest.md)
 - [Element](./Element.md)
 - [FixedChar](./FixedChar.md)
 - [FixedCharChar](./FixedCharChar.md)
 - [GSDataType](./GSDataType.md)
 - [GetEdgeType](./GetEdgeType.md)
 - [GetGraphResponse](./GetGraphResponse.md)
 - [GetGraphSchemaResponse](./GetGraphSchemaResponse.md)
 - [GetProcedureResponse](./GetProcedureResponse.md)
 - [GetPropertyMeta](./GetPropertyMeta.md)
 - [GetVertexType](./GetVertexType.md)
 - [JobResponse](./JobResponse.md)
 - [JobStatus](./JobStatus.md)
 - [KeyValue](./KeyValue.md)
 - [LongText](./LongText.md)
 - [ModelProperty](./ModelProperty.md)
 - [Parameter](./Parameter.md)
 - [PrimitiveType](./PrimitiveType.md)
 - [PropertyArray](./PropertyArray.md)
 - [QueryRequest](./QueryRequest.md)
 - [Record](./Record.md)
 - [SchemaMapping](./SchemaMapping.md)
 - [SchemaMappingLoadingConfig](./SchemaMappingLoadingConfig.md)
 - [SchemaMappingLoadingConfigFormat](./SchemaMappingLoadingConfigFormat.md)
 - [ServiceStatus](./ServiceStatus.md)
 - [StartServiceRequest](./StartServiceRequest.md)
 - [StoredProcedureMeta](./StoredProcedureMeta.md)
 - [StringType](./StringType.md)
 - [StringTypeString](./StringTypeString.md)
 - [TemporalType](./TemporalType.md)
 - [TimeStampType](./TimeStampType.md)
 - [TypedValue](./TypedValue.md)
 - [UpdateProcedureRequest](./UpdateProcedureRequest.md)
 - [VarChar](./VarChar.md)
 - [VarCharVarChar](./VarCharVarChar.md)
 - [VertexData](./VertexData.md)
 - [VertexMapping](./VertexMapping.md)
 - [VertexRequest](./VertexRequest.md)


<a id="documentation-for-authorization"></a>
## Documentation For Authorization

Authentication is not supported yet, and we will be introducing authorization-related implementation in the near future.




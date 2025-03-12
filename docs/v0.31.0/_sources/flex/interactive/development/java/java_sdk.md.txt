# Java SDK Reference

The Interactive Java SDK Reference is a comprehensive guide for developers looking to integrate the Interactive service into their Java applications. This SDK allows users to seamlessly connect to Interactive and leverage its powerful features for graph management, stored procedure management, and query execution.


## Requirements

Building the API client library requires:
1. Java 1.8+
2. Maven (3.8.3+)/Gradle (7.2+)

## Installation

To install the API client library to your local Maven repository, simply execute:

```shell
git clone https://github.com/alibaba/GraphScope.git
cd GraphScope/flex/interactive/sdk/java
mvn clean install
```

To deploy it to a remote Maven repository instead, configure the settings of the repository and execute:

```shell
mvn clean deploy
```

Refer to the [OSSRH Guide](http://central.sonatype.org/pages/ossrh-guide.html) for more information.

### Maven users

Add this dependency to your project's POM:

```xml
<dependency>
  <groupId>com.alibaba.graphscope</groupId>
  <artifactId>interactive-sdk</artifactId>
  <version>0.3</version>
</dependency>
```

### Others

At first generate the JAR by executing:

```shell
mvn clean package
```

Then manually install the following JARs:

* `target/interactive-sdk-0.3.jar`
* `target/lib/*.jar`

## Getting Started

First, install and start the interactive service via [Interactive Getting Started](https://graphscope.io/docs/flex/interactive/getting_started), and you will get the all the endpoints for the Interactive service.

```bash
You can connect to Interactive service with Interactive SDK, with following environment variables declared.

############################################################################################
    export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:7777
    export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:10000
    export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:7687
############################################################################################
```

```{note}
If you have customized the ports when deploying Interactive, remember to replace the default ports with your customized ports.
```

Remember to export these environment variables.

### Connect and submit a query

Interactive provide you with a default graph, `modern_graph`. You can connect to the interactive endpoint, and try to run a simple query with following code.

```java
package com.alibaba.graphscope;

import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.models.*;

public class GettingStarted {
    public static void main(String[] args) {
        Driver driver = Driver.connect();
        Session session = driver.session();

        // start a query
        // run cypher query
        try (org.neo4j.driver.Session neo4jSession = driver.getNeo4jSession()) {
            org.neo4j.driver.Result result = neo4jSession.run("MATCH(a) return COUNT(a);");
            System.out.println("result: " + result.toString());
        }
        return;
    }
}
```

### Create a new graph

To create a new graph, user need to specify the name, description, vertex types and edges types.
For the detail data model of the graph, please refer to [Data Model](../../data_model). 

In this example, we will create a simple graph with only one vertex type `persson`, and one edge type named `knows`.

```java
public class GettingStarted {
    private static final String TEST_GRAPH_SCHEMA_JSON = "{\n" +
            "    \"name\": \"test_graph\",\n" +
            "    \"description\": \"This is a test graph\",\n" +
            "    \"schema\": {\n" +
            "        \"vertex_types\": [\n" +
            "            {\n" +
            "                \"type_name\": \"person\",\n" +
            "                \"properties\": [\n" +
            "                    {\n" +
            "                        \"property_name\": \"id\",\n" +
            "                        \"property_type\": {\"primitive_type\": \"DT_SIGNED_INT64\"},\n" +
            "                    },\n" +
            "                    {\n" +
            "                        \"property_name\": \"name\",\n" +
            "                        \"property_type\": {\"string\": {\"long_text\": \"\"}},\n" +
            "                    },\n" +
            "                    {\n" +
            "                        \"property_name\": \"age\",\n" +
            "                        \"property_type\": {\"primitive_type\": \"DT_SIGNED_INT32\"},\n" +
            "                    },\n" +
            "                ],\n" +
            "                \"primary_keys\": [\"id\"],\n" +
            "            }\n" +
            "        ],\n" +
            "        \"edge_types\": [\n" +
            "            {\n" +
            "                \"type_name\": \"knows\",\n" +
            "                \"vertex_type_pair_relations\": [\n" +
            "                    {\n" +
            "                        \"source_vertex\": \"person\",\n" +
            "                        \"destination_vertex\": \"person\",\n" +
            "                        \"relation\": \"MANY_TO_MANY\",\n" +
            "                    }\n" +
            "                ],\n" +
            "                \"properties\": [\n" +
            "                    {\n" +
            "                        \"property_name\": \"weight\",\n" +
            "                        \"property_type\": {\"primitive_type\": \"DT_DOUBLE\"},\n" +
            "                    }\n" +
            "                ],\n" +
            "                \"primary_keys\": [],\n" +
            "            }\n" +
            "        ],\n" +
            "    },\n" +
            "}";

    public static String createGraph(Session session) throws IOException {
        CreateGraphRequest graph = CreateGraphRequest.fromJson(TEST_GRAPH_SCHEMA_JSON);
        Result<CreateGraphResponse> rep = session.createGraph(graph);
        if (rep.isOk()) {
            System.out.println("create graph success");
        } else {
            throw new RuntimeException("create graph failed: " + rep.getStatusMessage());
        }
        String graphId = rep.getValue().getGraphId();
        System.out.println("graphId: " + graphId);
        return graphId;
    }

    public static void main(String[] args) {
        //Previous code...
        Session session = driver.session();
        String graphId = createGraph(sess);
        return ;
    }
}
```

In the above example, a graph with name `test_graph` is defined via a json string. You can also define the graph with programmatic interface provided by [CreateGraphRequest](./CreateGraphRequest.md). So you call the method `createGraph`, a string reprensents the unique identifier of the graph is returned.


### Import data to the graph

After a new graph is created, you may want to import data into the newly created graph. 
For the detail configuration of data import, please refer to [Data Import Configuration](../../data_import).

For example, you can import the local csv files into the `test_graph`. Note that, currently only csv files are supported now. Remember to replase `/path/to/person.csv` and `/path/to/person_knows_person.csv` with the actual local path. You can download them from [GraphScope Interactive Github reop](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/modern_graph).

```java
public class GettingStarted {
    //Remember to replace the path with your own file path
    private static final String TEST_GRAPH_BULK_LOADING_JSON = "{\n" +
            "    \"vertex_mappings\": [\n" +
            "        {\n" +
            "            \"type_name\": \"person\",\n" +
            "            \"inputs\": [\"@/path/to/person.csv\"],\n" +
            "            \"column_mappings\": [\n" +
            "                {\"column\": {\"index\": 0, \"name\": \"id\"}, \"property\": \"id\"},\n" +
            "                {\"column\": {\"index\": 1, \"name\": \"name\"}, \"property\": \"name\"},\n" +
            "                {\"column\": {\"index\": 2, \"name\": \"age\"}, \"property\": \"age\"},\n" +
            "            ],\n" +
            "        }\n" +
            "    ],\n" +
            "    \"edge_mappings\": [\n" +
            "        {\n" +
            "            \"type_triplet\": {\n" +
            "                \"edge\": \"knows\",\n" +
            "                \"source_vertex\": \"person\",\n" +
            "                \"destination_vertex\": \"person\",\n" +
            "            },\n" +
            "            \"inputs\": [\n" +
            "                \"@/path/to/person_knows_person.csv\"\n" +
            "            ],\n" +
            "            \"source_vertex_mappings\": [\n" +
            "                {\"column\": {\"index\": 0, \"name\": \"person.id\"}, \"property\": \"id\"}\n" +
            "            ],\n" +
            "            \"destination_vertex_mappings\": [\n" +
            "                {\"column\": {\"index\": 1, \"name\": \"person.id\"}, \"property\": \"id\"}\n" +
            "            ],\n" +
            "            \"column_mappings\": [\n" +
            "                {\"column\": {\"index\": 2, \"name\": \"weight\"}, \"property\": \"weight\"}\n" +
            "            ],\n" +
            "        }\n" +
            "    ],\n" +
            "}";

    public static void bulkLoading(Session session, String graphId) throws IOException {
        SchemaMapping schemaMapping = SchemaMapping.fromJson(TEST_GRAPH_BULK_LOADING_JSON);
        Result<JobResponse> rep = session.bulkLoading(graphId, schemaMapping);
        if (rep.isOk()) {
            System.out.println("Bulk loading success");
        } else {
            throw new RuntimeException("bulk loading failed: " + rep.getStatusMessage());
        }
        String jobId = rep.getValue().getJobId();
        System.out.println("job id: " + jobId);
        // Wait job finish
        while (true) {
            Result<JobStatus> rep = session.getJobStatus(jobId);
            if (!rep.isOk()) {
                throw new RuntimeException("get job status failed: " + rep.getStatusMessage());
            }
            JobStatus job = rep.getValue();
            if (job.getStatus() == JobStatus.StatusEnum.SUCCESS) {
                System.out.println("job finished");
                break;
            } else if (job.getStatus() == JobStatus.StatusEnum.FAILED) {
                throw new RuntimeException("job failed");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        //Previous code...
        Session session = driver.session();
        String graphId = createGraph(sess);
        bulkLoading(sess, graphId);
        return ;
    }
}
```

For each vertex/edge types, you need to provide the input data source and column mapping infomation.
Remember to add `@` at the begining of the local file path. 
`Session.bulkLoading()` will submit an dataloading job to the service, and we can query the status of the job via `Session.getJobStatus()`, and wait until the job has compleleted successfully.

### Create a stored procedure

Stored procedures can be registered into GraphScope Interactive to encapsulate and reuse complex graph operations. Interactive support both `cypher` and `c++` queries as stored procedures. 
With the following code, you will create a procedure named `testProcedure` which is definied via a `cypher` query.

```java
public class GettingStarted{
    public static String createProcedure(Session sess, String graphId) {
        String procName = "testProcedure";
        CreateProcedureRequest procedure =
                new CreateProcedureRequest()
                        .name(procName)
                        .description("a simple test procedure")
                        .query("MATCH(p:person) RETURN COUNT(p);")
                        .type(CreateProcedureRequest.TypeEnum.CYPHER);
        Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
        if (resp.isOk()) {
            System.out.println("create procedure success");
        } else {
            throw new RuntimeException("create procedure failed: " + resp.getStatusMessage());
        }
        return procName;
    }

    public static void main(String[] args) {
        //Previous code...
        Session session = driver.session();
        String graphId = createGraph(sess);
        bulkLoading(sess, graphId);
        String procName = createProcedure(sess, graphId);
        return ;
    }
}
```

The procedure could not be invokded now, since currently interactive service has not been switched to the newly created `test_graph`. We need to start the service on `test_graph`.

### Start the query service on the new graph

Although Interactive supports multiple graphs in terms of logic and storage, it can only serve on one graph at a time. This means that at any given moment, only one graph is available to provide query service. So we need to switch to the newly created `test_graph` with following code.

```java
public class GettingStarted{
    public static void startService(Session sess, String graphId){
        Result<String> startServiceResponse =
            session.startService(new StartServiceRequest().graphId(graphId));
        if (startServiceResponse.isOk()) {
            System.out.println("start service success");
        } else {
            throw new RuntimeException(
                    "start service failed: " + startServiceResponse.getStatusMessage());
        }
    }

    public static void main(String[] args) {
        //Previous code...
        Session session = driver.session();
        String graphId = createGraph(sess);
        bulkLoading(sess, graphId);
        String procName = createProcedure(sess, graphId);
        starService(sess, graphId);
        return ;
    }
}
```

### Submit queries to the new graph

After starting query service on the new graph, we are now able to submit queries to `test_graph`.

<!-- #### Submit gremlin queries

```java
public class GettingStarted{
    public static void main(String[] args) {
        // Previous code...
        // run gremlin query
        Client gremlinClient = driver.getGremlinClient();
        try {
            List<org.apache.tinkerpop.gremlin.driver.Result> results =
                    gremlinClient.submit("g.V().count()").all().get();
            System.out.println("result: " + results.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ;
    }
}
``` -->

#### Submit cypher queries

```java
public class GettingStarted{
    public static void main(String[] args) {
        // Previous code...
        // run cypher query
        try (org.neo4j.driver.Session neo4jSession = driver.getNeo4jSession()) {
            org.neo4j.driver.Result result = neo4jSession.run("MATCH(a) return COUNT(a);");
            System.out.println("result: " + result.toString());
        }
        return ;
    }
}
```

#### Query the stored procedure

```java
public class GettingStarted{
    public static void main(String[] args) {
        // Previous code...
        // call the stored procedure
        try (org.neo4j.driver.Session neo4jSession = driver.getNeo4jSession()) {
            org.neo4j.driver.Result result = neo4jSession.run("CALL testProcedure() YIELD *;");
            System.out.println("result: " + result.toString());
        }
        return ;
    }
}
```

### Delete the graph

Finally, we can delete the graph. The graph data and stored procedure bound to the graph will also be deleted.

```java
public class GettingStarted{
    public static void main(String[] args) {
        // Previous code...
        Result<String> deleteGraphResponse = session.deleteGraph(graphId);
        if (deleteGraphResponse.isOk()) {
            System.out.println("delete graph success");
        } else {
            throw new RuntimeException("delete graph failed: " + deleteGraphResponse.getStatusMessage());
        }
        return ;
    }
}
```

For the full example, please refer to [Java SDK Example](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/sdk/examples/java/interactive-example)

## Documentation for Service APIs

The APIs in interactive SDK are divided into five categories.
- GraphManagementApi
- ProcedureManagementApi
- JobManagementApi
- ServiceManagementApi
- QueryServiceApi
- VertexApi
- EdgeApi



All URIs are relative to `${INTERACTIVE_ADMIN_ENDPOINT}`

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*GraphManagementApi* | [**BulkLoading**](./GraphManagementApi.md#Bulkloading) | **POST** /v1/graph/{graph_id}/dataloading | 
*GraphManagementApi* | [**CreateGraph**](./GraphManagementApi.md#CreateGraph) | **POST** /v1/graph | 
*GraphManagementApi* | [**DeleteGraph**](./GraphManagementApi.md#DeleteGraph) | **DELETE** /v1/graph/{graph_id} | 
*GraphManagementApi* | [**GetGraphMeta**](./GraphManagementApi.md#GetGraphMeta) | **GET** /v1/graph/{graph_id} | 
*GraphManagementApi* | [**GetGraphSchema**](./GraphManagementApi.md#GetGraphSchema) | **GET** /v1/graph/{graph_id}/schema | 
*GraphManagementApi* | [**ListGraphs**](./GraphManagementApi.md#ListGraphs) | **GET** /v1/graph | 
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
*VertexApi* | [**addVertex**](./VertexApi.md#addVertex) | **POST** /v1/graph/{graph_id}/vertex | Add vertex to the graph
*VertexApi* | [**getVertex**](./VertexApi.md#getVertex) | **GET** /v1/graph/{graph_id}/vertex | Get the vertex&#39;s properties with vertex primary key.
*VertexApi* | [**updateVertex**](./VertexApi.md#updateVertex) | **PUT** /v1/graph/{graph_id}/vertex | Update vertex&#39;s property
*EdgeApi* | [**addEdge**](./EdgeApi.md#addEdge) | **POST** /v1/graph/{graph_id}/edge | Add edge to the graph
*EdgeApi* | [**getEdge**](./EdgeApi.md#getEdge) | **GET** /v1/graph/{graph_id}/edge | Get the edge&#39;s properties with src and dst vertex primary keys.
*EdgeApi* | [**updateEdge**](./EdgeApi.md#updateEdge) | **PUT** /v1/graph/{graph_id}/edge | Update edge&#39;s property


```{note}
Delete Vertex/Edge is currently not supported by Interactive.
```

## Documentation for Utilities APIs

In addition to the documentation for service APIs, we also offer documentation for [Utility APIs](./reference/index.rst).

- [Driver](./reference/com/alibaba/graphscope/interactive/client/Driver.rst)
- [Session](./reference/com/alibaba/graphscope/interactive/client/Session.rst)
- [DefaultSession](./reference/com/alibaba/graphscope/interactive/client/impl/DefaultSession.rst)
- [Status](./reference/com/alibaba/graphscope/interactive/client/common/Status.rst)
- [Result](./reference/com/alibaba/graphscope/interactive/client/common/Result.rst)
- [Encoder](./reference/com/alibaba/graphscope/interactive/client/utils/Encoder.rst)
- [Decoder](./reference/com/alibaba/graphscope/interactive/client/utils/Encoder.rst)


## Documentation for Data Structures

 - [APIResponseWithCode](./APIResponseWithCode.md)
 - [BaseEdgeType](./BaseEdgeType.md)
 - [BaseEdgeTypeVertexTypePairRelationsInner](./BaseEdgeTypeVertexTypePairRelationsInner.md)
 - [BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams](./BaseEdgeTypeVertexTypePairRelationsInnerXCsrParams.md)
 - [BasePropertyMeta](./BasePropertyMeta.md)
 - [BaseVertexType](./BaseVertexType.md)
 - [BaseVertexTypeXCsrParams](./BaseVertexTypeXCsrParams.md)
 - [ColumnMapping](./ColumnMapping.md)
 - [CreateEdgeType](./CreateEdgeType.md)
 - [CreateGraphRequest](./CreateGraphRequest.md)
 - [CreateGraphResponse](./CreateGraphResponse.md)
 - [CreateGraphSchemaRequest](./CreateGraphSchemaRequest.md)
 - [CreateProcedureRequest](./CreateProcedureRequest.md)
 - [CreateProcedureResponse](./CreateProcedureResponse.md)
 - [CreatePropertyMeta](./CreatePropertyMeta.md)
 - [CreateVertexType](./CreateVertexType.md)
 - [DateType](./DateType.md)
 - [EdgeData](./EdgeData.md)
 - [EdgeMapping](./EdgeMapping.md)
 - [EdgeMappingDestinationVertexMappingsInner](./EdgeMappingDestinationVertexMappingsInner.md)
 - [EdgeMappingSourceVertexMappingsInner](./EdgeMappingSourceVertexMappingsInner.md)
 - [EdgeMappingSourceVertexMappingsInnerColumn](./EdgeMappingSourceVertexMappingsInnerColumn.md)
 - [EdgeMappingTypeTriplet](./EdgeMappingTypeTriplet.md)
 - [EdgeRequest](./EdgeRequest.md)
 - [EdgeStatistics](./EdgeStatistics.md)
 - [FixedChar](./FixedChar.md)
 - [FixedCharChar](./FixedCharChar.md)
 - [GSDataType](./GSDataType.md)
 - [GetEdgeType](./GetEdgeType.md)
 - [GetGraphResponse](./GetGraphResponse.md)
 - [GetGraphSchemaResponse](./GetGraphSchemaResponse.md)
 - [GetGraphStatisticsResponse](./GetGraphStatisticsResponse.md)
 - [GetProcedureResponse](./GetProcedureResponse.md)
 - [GetPropertyMeta](./GetPropertyMeta.md)
 - [GetVertexType](./GetVertexType.md)
 - [JobResponse](./JobResponse.md)
 - [JobStatus](./JobStatus.md)
 - [LongText](./LongText.md)
 - [Parameter](./Parameter.md)
 - [PrimitiveType](./PrimitiveType.md)
 - [Property](./Property.md)
 - [QueryRequest](./QueryRequest.md)
 - [SchemaMapping](./SchemaMapping.md)
 - [SchemaMappingLoadingConfig](./SchemaMappingLoadingConfig.md)
 - [SchemaMappingLoadingConfigDataSource](./SchemaMappingLoadingConfigDataSource.md)
 - [SchemaMappingLoadingConfigFormat](./SchemaMappingLoadingConfigFormat.md)
 - [SchemaMappingLoadingConfigXCsrParams](./SchemaMappingLoadingConfigXCsrParams.md)
 - [ServiceStatus](./ServiceStatus.md)
 - [StartServiceRequest](./StartServiceRequest.md)
 - [StoredProcedureMeta](./StoredProcedureMeta.md)
 - [StringType](./StringType.md)
 - [StringTypeString](./StringTypeString.md)
 - [TemporalType](./TemporalType.md)
 - [TemporalTypeTemporal](./TemporalTypeTemporal.md)
 - [TimeStampType](./TimeStampType.md)
 - [TypedValue](./TypedValue.md)
 - [UpdateProcedureRequest](./UpdateProcedureRequest.md)
 - [UploadFileResponse](./UploadFileResponse.md)
 - [VarChar](./VarChar.md)
 - [VarCharVarChar](./VarCharVarChar.md)
 - [VertexData](./VertexData.md)
 - [VertexEdgeRequest](./VertexEdgeRequest.md)
 - [VertexMapping](./VertexMapping.md)
 - [VertexRequest](./VertexRequest.md)
 - [VertexStatistics](./VertexStatistics.md)
 - [VertexTypePairStatistics](./VertexTypePairStatistics.md)


<a id="documentation-for-authorization"></a>
## Documentation for Authorization

Authentication is not supported yet, and we will be introducing authorization-related implementation in the near future.



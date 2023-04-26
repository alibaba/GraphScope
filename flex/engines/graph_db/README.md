# Graph Database Engine

## 1. Introduction

`graph_db` provides a graph database engine. It supports concurrent transactional operations.

## 2. Transactions

### 2.1 Read Transaction

`GraphDB::GetReadTransaction()` returns a `ReadTransaction` object.

With an `ReadTransaction`, a specific version of the graph can be read. The version is determined by the timestamp of the transaction.

`ReadTransaction` provides a set of APIs to read the graph, including schema, topology, and properties.

After query with the `ReadTransaction` object, the transaction should be released by calling `ReadTransaction::Release()`.

### 2.2 Insert Transaction

`GraphDB::GetInsertTransaction()` returns an `InsertTransaction` object.

With an `InsertTransaction`, a set of vertices and edges can be inserted into the graph with the timestamp of transaction.

After insertion, the transaction can be committed by calling `InsertTransaction::Commit()` or be aborted by calling `InsertTransaction::Abort()`.

`InsertTransaction` does not provide interfaces to read the graph.

### 2.3 Update Transaction

`GraphDB::GetUpdateTransaction()` returns an `UpdateTransaction` object.

With an `UpdateTransaction`, a specific version of the graph can be read. The version is determined by the timestamp of the transaction.

Also, `UpdateTransaction` provides interfaces to insert and update vertices and edges.

After insertion and update, the transaction can be committed by calling `UpdateTransaction::Commit()` or be aborted by calling `UpdateTransaction::Abort()`.

## 3. Version Management

### 3.1 Visibility

Each edge is associated with a timestamp, which is the timestamp of the transaction that inserts the edge.

When read graph with a `ReadTransaction` or `UpdateTransaction`, only edges with timestamp less than or equal to the timestamp of the transaction will be returned.

### 3.2 Syncrhonization

There is no synchronization between read and insert transactions. All read and insert transactions can be executed concurrently.

When an `UpdateTransaction` is created, it will wait for all read and insert transactions to finish. Then, the `UpdateTransaction` will read the graph with the latest timestamp, and all read and insert transactions will be blocked until the `UpdateTransaction` is committed or aborted.

### 3.3 Serializability

For a `ReadTransaction`, it will be assigned with a timestamp of the graph, all insert or update transactions with timestamp less than or equal to the timestamp have been committed.

For each `InsertTransaction` or `UpdateTransaction`, a unique timestamp will be assigned. When committing, a write-ahead log will be written to the disk and all modifications will be applied to the graph atomically.

## 4. Stored Procedures

Stored procedures can only be registered to the engine in the initializing phase through graph schema yaml. They can be invoked by the client or http requests.

```cpp

class TraverseStoredProcedure : public AppBase {
public:
    TraverseStoredProcedure(GraphDBSession& graph) :
    person_label_id_(graph.schema().get_vertex_label_id("person"),
                     software_label_id_(graph.schema().get_vertex_label_id("software")),
                     created_label_id_(graph.schema().get_edge_label_id("created")),
                     software_name_col_(*(std::dynamic_pointer_cast<StringColumn>(
                        graph.get_vertex_property_column(software_label_id_, "name")))),
                     graph_(graph) {}
    ~TraverseStoredProcedure {}
    
    bool Query(Encoder& input, Encoder& output) override {
        auto txn = graph_.GetReadTransaction();
        oid_t person_id = input.get_long();
        vid_t root{};
        if (!txn.GetVertexIndex(person_label_id_, person_id, root)) {
            return false;
        }
        auto person_created_software_out = txn.GetOutgoingGraphView<double>(
                person_label_id_, software_label_id_, created_label_id_);
        auto oe = person_created_software_out.get_edges(root);
        for (auto& e : oe) {
            auto software = e.neighbor;
            output.put_string(software_name_col_.get_view(software));
        }
        txn.Release();
        return true;
    }
private:
    GraphDBSession& graph_;
    label_t person_label_id_;
    label_t software_label_id_;
    label_t created_label_id_;
};

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  TraverseStoredProcedure* app = new TraverseStoredProcedure(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  TraverseStoredProcedure* casted = static_cast<TraverseStoredProcedure*>(app);
  delete casted;
}
}
```

This is an example of read transaction stored procedure. It traverses the graph and returns the names of the software created by a person.

The stored procedure should be compiled into a shared library and then be loaded by the graph database engine.

## 5. Server

`rt_server` initializes graph storage through bulk loading from raw files or recovering from a database directory (contains initial snapshot and write-ahead logs).

After loading, it starts a server to accept http requests from clients.

There are two kinds of requests:

- Stored procedure management requests: register, and list stored procedures
- Query requests: query the graph with a stored procedure

```
rt_server -g /path/to/schema_config -d /path/to/data [-l /path/to/load_schema] [-s n]
```

- `-d` Specifies the work directory, snapshots and write-ahead log will be generated here.
    - If the work directory is empty, the query engine will load graph defined by the input description file, and generate an initial snapshot in work directory.
    - Otherwise, the query engine will recover by loading the initial snapshot and replaying update operations.
- `-g` Specifies the schema description file.
    - `storages/rt_mutable_graph/modern_graph/modern_graph.yaml` is an example of schema description file.
- `-l` Specifies the bulk loading description file.
    - `storages/rt_mutable_graph/modern_graph/bulk_load.yaml` is an example of bulk loading description file, it defines the path of raw files and the mapping from raw files to vertex/edge labels.
- `-s` Specifies the concurrency.

Recovering from an existing work directory and then bulk loading from raw files is not supported. When the specified work directory is recoverable and the bulk loading description file is provided, the bulk loading will be ignored.

## 5. Client (Admin)

`rt_admin` is a tool for managing stored procedures and inspecting graph data. The corresponding `rt_admin` service can be configured with environment variables `GRAPHSCOPE_IP` and `GRAPHSCOPE_PORT`. It currently supports 5 types of operations.

```
rt_admin <operation> <args...>
```

- Show all registered stored procedures:

```
rt_admin show_stored_procedures
```

- Query vertex by providing label and id:

```
rt_admin query_vertex <LABEL> <ID>
```

- Query edges by providing labels and ids:

```
rt_admin query_edge <SRC-LABEL> <SRC-ID> <DST-LABEL> <DST-ID> <EDGE-LABEL>
```

Labels can be specified as `_ANY_LABEL` and ids can be specified as `_ANY_ID` to query a set of edges.






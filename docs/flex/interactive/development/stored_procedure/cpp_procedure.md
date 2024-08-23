# Create c++ Stored Procedures on GraphScope Interactive

Apart from adapting Cypher query as stored procedure, Interactive supports implementing stored procedure via c++ code, by calling storage interface.

## Getting Started.

We take a example to demonstrate how to create a stored procedure in c++. 
In this example, we implement a procedure which returns the total number of vertices.

Before proceed, make sure you have installed and launched Interactive as describe in [Getting Started](../../getting_started.md), and environment variables are correctly exported.

### Define a C++ Stored Procedure

A C++ stored procedure should be defined in a file, for example `count_vertices.cc`.

```c++
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/utils/app_utils.h"

namespace gs {

// A sample app get the count of the specified vertex label, since no write operations are needed
// we inherit from ReadAppBase. Otherwise you could inherit from WriteAppBase.
class CountVertices : public ReadAppBase {
 public:
  CountVertices() {}
  /**
   * @brief Query function for query class. 
   * @param sess: GraphDBSession The interface where you can visit the graph.
   * @param input: Decoder From where you could deserialize the input parameters.
   * @param output: Encoder To where you should encode the output parameters.
   */
  bool Query(const gs::GraphDBSession& sess, Decoder& input,
             Encoder& output) override {
    // First get the read transaction.
    auto txn = sess.GetReadTransaction();
    // We expect one param of type string from decoder.
    if (input.empty()){
        return false;
    }
    auto label_name = decoder.get_string();
    const auto& schema = txn.schema();
    if (!schema.has_vertex_label(label_name)){
        return false; // The requested label doesn't exits.
    }
    auto label_id = schema.get_vertex_label_id(label_name);
    // The vertices are labeled internally from 0 ~ vertex_label_num, accumulate the count.
    encoder.put_int(txn.GetVertexNum(label_id));
    txn.Commit();
    return true;
  }
};
}  // namespace gs

extern "C" {

// Defines how a instance of your procedure is created.
void* CreateApp(gs::GraphDBSession& db) {
  gs::CountVertices* app = new gs::CountVertices();
  return static_cast<void*>(app);
}

// Defines how a instance of your procedure should be deleted.
void DeleteApp(void* app) {
  gs::CountVertices* casted = static_cast<gs::CountVertices*>(app);
  delete casted;
}
}
```

### Register and Call the stored procedure

With the above `CountVertices` procedure defined, we could create a stored procedure from Interactive Python/Java SDK, or `gsctl`.

#### Python SDK

With [Interactive Python SDK](../python/python_sdk.md) Installed, you could easily create the stored procedure via the following code.

```python
from gs_interactive.client.driver import Driver
from gs_interactive.models import *

driver = Driver() # connecting to Interactive service, assuming environment variables like INTERACTIVE_ADMIN_ENDPOINT have been correctly exported.
sess = driver.session() # create the session

# Read the content of the procedure <count_vertices.cc>
app_path='/path/to/count_vertices.cc' # Replace the path with the real path to count_vertices.cc
if not os.path.exists(app_path):
    raise Exception("count_vertices.cc not found")
with open(app_path, "r") as f:
    app_content = f.read()

# we try to create the procedure on default graph, you may try to create on your customized graph by changing the graph_id 
graph_id = '1'

create_proc_request = CreateProcedureRequest(
    name="count_vertices",
    description="Count vertices for the specified vertex label name",
    query=app_content,
    type="cpp",
)
resp = sess.create_procedure(graph_id, create_proc_request)
print(resp)
assert resp.is_ok()
```

For more tails about Python SDK Interface, please refer to [Java SDK Procedure API](../python/ProcedureManagementApi.md).

#### Java SDk

With [Interactive Java SDK](../python/java_sdk.md) Installed, you could easily create the stored procedure via the following code.

```java
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.models.*;
import com.alibaba.graphscope.interactive.client.common.Result;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.net.URISyntaxException;

public class Test{
    public static void createProcedure(Session sess, String graphId) {
        CreateProcedureRequest procedure = new CreateProcedureRequest();
        procedure.setName("count_vertices");
        procedure.setDescription("Count vertices for the specified vertex label name");
        String appFilePath = "/path/to/count_vertices.cc"; // Please replace with the real path to count_vertices.cc
        String appFileContent = "";
        try {
            appFileContent =
                    new String(
                            Files.readAllBytes(
                                    Paths.get(
                                            Thread.currentThread()
                                                    .getContextClassLoader()
                                                    .getResource(appFilePath)
                                                    .toURI())));
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        if (appFileContent.isEmpty()) {
            throw new RuntimeException("sample app content is empty");
        }
        procedure.setQuery(appFileContent);
        procedure.setType(CreateProcedureRequest.TypeEnum.CPP);
        Result<CreateProcedureResponse> resp = session.createProcedure(graphId, procedure);
        if (!result.isOk()) {
            throw new RuntimeException("Fail to create procedure" + result.getStatusMessage());
        }
    }

    public static void main(String[] args) {
        Driver driver = Driver.connect();
        Session sess = driver.session();
        createProcedure(sess, "1");
        return ;
    }
}
```

For more tails about Java SDK Interface, please refer to [Java SDK Procedure API](../java/ProcedureManagementApi.md).


#### gsctl 

TODO


## Storage Interface

Interactive follow the design of Transaction, user should either make use of [`ReadTransaction`](https://github.com/alibaba/GraphScope/blob/main/flex/engines/graph_db/database/read_transaction.h), [`InsertTraction`](https://github.com/alibaba/GraphScope/blob/main/flex/engines/graph_db/database/insert_transaction.h) or [`UpdateTransaction`](https://github.com/alibaba/GraphScope/blob/main/flex/engines/graph_db/database/update_transaction.h).


## Query Input and Output

Interactive natively support two kind of protocols for param encoding and result decoding, `Encoder/Decoder` and `CypherApp`. 


### Encoder/Decoder

`Encoder/Decoder` based method provides the best performance. The serialization/deserialization could be customized by the user.

For the detail implementation, please ref to [app_utils.cc](https://github.com/alibaba/GraphScope/blob/main/flex/utils/app_utils.cc).

### CypherApp

If you want to call c++ procedures from neo4j client, then `CypherApp` is your way.
By inherit from `CypherReadAppBase` and returning the query result in `CollectiveResults`, it is possible for you to call the procedure from neo4j native client, like cypher-shell or neo4j java/python SDKs.

For example, the following stored procedure scan vertices with the specified label name, and return the property `id` for first 5 vertices of the specified vertex label.

```c++
#include "flex/engines/hqps_db/app/interactive_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {
class ExampleQuery : public CypherReadAppBase<std::string> {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  ExampleQuery() {}
  // Query function for query class
  results::CollectiveResults Query(const gs::GraphDBSession& sess,
                    std::string label_name) override {
    LOG(INFO) <<"Scan vertices with label name: " << label_name;
    const auto& schema = sess.schema();
    if (!schema.has_vertex_label(label_name)){
        return results::CollectiveResults();
    }
    label_t label_id = schema.get_vertex_label_id(label_name);
    gs::MutableCSRInterface graph(sess);
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, label_id, Filter<TruePredicate>());

    auto ctx1 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx0),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
            gs::PropertySelector<int64_t>("id"))});
    auto ctx2 = Engine::Limit(std::move(ctx1), 0, 5);
    auto res = Engine::Sink(graph, ctx2, std::array<int32_t, 1>{0});
    LOG(INFO) << "res: " << res.DebugString();
    return res;
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::ExampleQuery* app = new gs::ExampleQuery();
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::ExampleQuery* casted = static_cast<gs::ExampleQuery*>(app);
  delete casted;
}
}
```


Then after creating the stored procedure via Interactive Java/Python SDK, you could call the stored procedure from neo4j-native tools. For example, create the procedure with name `scan_vertices`, then in cypher shell

```cypher
CALL scan_vertices("person") YIELD *;
```




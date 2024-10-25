# Create c++ Stored Procedures on GraphScope Interactive

Apart from adapting [Cypher query as stored procedure](../../stored_procedures.md), Interactive supports implementing stored procedure via c++ code, by calling the interfaces provided by Graph Database Engine.

## Getting Started.

We take a example to demonstrate how to create a stored procedure in c++. 
In this example, we implement a procedure which returns the total number of vertices.

Before proceed, make sure you have installed and launched Interactive as describe in [Getting Started](../../getting_started.md), and environment variables are correctly exported.

### Define a C++ Stored Procedure

A C++ stored procedure should be defined in a file, for example `count_vertices.cc`.

```c++
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/utils/app_utils.h"

namespace gs {

// A sample app get the count of the specified vertex label, since no write
// operations are needed we inherit from ReadAppBase. Otherwise you could
// inherit from WriteAppBase.
class CountVertices : public ReadAppBase {
public:
  CountVertices() {}
  /**
   * @brief Query function for query class.
   * @param sess: GraphDBSession The interface where you can visit the graph.
   * @param input: Decoder From where you could deserialize the input
   * parameters.
   * @param output: Encoder To where you should encode the output parameters.
   */
  bool Query(const gs::GraphDBSession &sess, Decoder &input,
             Encoder &output) override {
    // First get the read transaction.
    auto txn = sess.GetReadTransaction();
    // We expect one param of type string from decoder.
    if (input.empty()) {
      return false;
    }
    std::string label_name{input.get_string()};
    const auto &schema = txn.schema();
    if (!schema.has_vertex_label(label_name)) {
      return false; // The requested label doesn't exits.
    }
    auto label_id = schema.get_vertex_label_id(label_name);
    // The vertices are labeled internally from 0 ~ vertex_label_num, accumulate
    // the count.
    output.put_int(txn.GetVertexNum(label_id));
    txn.Commit();
    return true;
  }
};
} // namespace gs

extern "C" {

// Defines how a instance of your procedure is created.
void *CreateApp(gs::GraphDBSession &db) {
  gs::CountVertices *app = new gs::CountVertices();
  return static_cast<void *>(app);
}

// Defines how a instance of your procedure should be deleted.
void DeleteApp(void *app) {
  gs::CountVertices *casted = static_cast<gs::CountVertices *>(app);
  delete casted;
}
}
```

### Register and Call the stored procedure

With the above `CountVertices` procedure defined, we could create a stored procedure with `gsctl` or use Interactive Python/Java SDKs.

#### gsctl 

With Interactive deployed, you can register a C++ stored procedure similarly to [creating a Cypher stored procedure](../../stored_procedures.md).

##### Define the YAML

When defined, C++ stored procedures' YAML differ from Cypher procedures only in the `type` field, i.e., `cpp` versus `cypher`. Users can include the stored procedure's implementation directly in the YAML file.


```yaml
name: test_procedure
description: "Ths is a test procedure"
query: |
    #include "flex/engines/graph_db/app/app_base.h"
    #include "flex/engines/graph_db/database/graph_db_session.h"
    #include "flex/utils/app_utils.h"

    namespace gs {

    // A sample app get the count of the specified vertex label, since no write
    // operations are needed we inherit from ReadAppBase. Otherwise you could
    // inherit from WriteAppBase.
    class CountVertices : public ReadAppBase {
    public:
    CountVertices() {}
    /**
    * @brief Query function for query class.
    * @param sess: GraphDBSession The interface where you can visit the graph.
    * @param input: Decoder From where you could deserialize the input
    * parameters.
    * @param output: Encoder To where you should encode the output parameters.
    */
    bool Query(const gs::GraphDBSession &sess, Decoder &input,
                Encoder &output) override {
        // First get the read transaction.
        auto txn = sess.GetReadTransaction();
        // We expect one param of type string from decoder.
        if (input.empty()) {
        return false;
        }
        std::string label_name{input.get_string()};
        const auto &schema = txn.schema();
        if (!schema.has_vertex_label(label_name)) {
        return false; // The requested label doesn't exits.
        }
        auto label_id = schema.get_vertex_label_id(label_name);
        // The vertices are labeled internally from 0 ~ vertex_label_num, accumulate
        // the count.
        output.put_int(txn.GetVertexNum(label_id));
        txn.Commit();
        return true;
    }
    };
    } // namespace gs

    extern "C" {

    // Defines how a instance of your procedure is created.
    void *CreateApp(gs::GraphDBSession &db) {
    gs::CountVertices *app = new gs::CountVertices();
    return static_cast<void *>(app);
    }

    // Defines how a instance of your procedure should be deleted.
    void DeleteApp(void *app) {
    gs::CountVertices *casted = static_cast<gs::CountVertices *>(app);
    delete casted;
    }
    }
type: cpp
```

You may find the c++ code is too long, and maybe hard to update, especially if some modifications are needed. Fortunately, we support uploading the procedure implementation from file, you just need to provide the full path of the c++ file, with `@` prepended. 


```yaml
name: test_procedure
description: "Ths is a test procedure"
query: "@/path/to/procedure.cc"
type: cpp
```


#### Python SDK

With [Interactive Python SDK](../python/python_sdk.md) Installed, you could easily create the stored procedure via the following code.

```python
import os
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

#### Java SDK

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

import org.junit.jupiter.api.Test;

public class CreateProcedureTest{
    public static void createProcedure(Session sess, String graphId) {
        CreateProcedureRequest procedure = new CreateProcedureRequest();
        procedure.setName("count_vertices");
        procedure.setDescription("Count vertices for the specified vertex label name");
        String appFilePath = "/path/to/count_vertices.cc"; // Please replace with the real path to count_vertices.cc
        // check file exist
        if (Files.notExists(Paths.get(appFilePath))) {
            throw new RuntimeException("sample app file not exist");
        }
        String appFileContent = "";
        try {
            appFileContent =
                    new String(
                            Files.readAllBytes(Paths.get(appFilePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (appFileContent.isEmpty()) {
            throw new RuntimeException("sample app content is empty");
        }
        procedure.setQuery(appFileContent);
        procedure.setType(CreateProcedureRequest.TypeEnum.CPP);
        Result<CreateProcedureResponse> resp = sess.createProcedure(graphId, procedure);
        if (!resp.isOk()) {
            throw new RuntimeException("Fail to create procedure" + resp.getStatusMessage());
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

## Create a Stored Procedure

First, switch to the graph where you want to create the procedure. We will use the built-in graph as an example. For instructions on creating your own graph, please refer to [Use Custom Graph](../../custom_graph_data.md).

```bash
gsctl use GRAPH gs_interactive_default_graph 
```

Then create the procedure with `gsctl`:

```bash
gsctl create storedproc -f ./procedure.yaml
```

This will initiate the compilation process to convert C++ code into a dynamic library, which may take a few seconds. After compilation, it is **necessary** to restart the service to activate the stored procedures.


```bash
gsctl service restart
```

## Graph Database Engine

Interactive follow the design of Transaction, user should either make use of [`ReadTransaction`](https://github.com/alibaba/GraphScope/blob/main/flex/engines/graph_db/database/read_transaction.h), [`InsertTraction`](https://github.com/alibaba/GraphScope/blob/main/flex/engines/graph_db/database/insert_transaction.h) or [`UpdateTransaction`](https://github.com/alibaba/GraphScope/blob/main/flex/engines/graph_db/database/update_transaction.h).

We recommend you to read though the code of [GraphDB](https://github.com/alibaba/GraphScope/tree/main/flex/engines/graph_db) for more detail, if you want to 
write a stored procedure with best performance. If you encounter any problems, feel free to contact us by submit issue or create discussions. 


## Query Input and Output

Interactive natively support two kind of protocols for param encoding and result decoding, `Encoder/Decoder` and `CypherApp`. 


### Encoder/Decoder

`Encoder/Decoder` based method provides the best performance. The serialization/deserialization could be customized by the user. 
In this serialization protocol, User need to take care of the param encoding and decoding himself. 
For example, the example procedure above `count_vertices.cc` use Encoder/Decoder to encode the input and decode the output.


Here is an example of how to query the `count_vertices` procedure using Interactive Java SDK and Python SDK. Please note that you need to switch to the graph with ID "1" in order to make the procedure callable.


```java
import com.alibaba.graphscope.interactive.client.Driver;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.models.*;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.client.utils.Encoder;
import com.alibaba.graphscope.interactive.client.utils.Decoder;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.jupiter.api.Test;

public class CreateProcedureTest{
    public static void callProcedure(Session sess, String graphId, String procedureName, String labelName) {
        byte[] bytes = new byte[1 + 4 + labelName.length()]; // 1 byte for procedure index, 4 bytes for label length, and labelName.length() bytes for label name
        Encoder encoder = new Encoder(bytes);
        encoder.put_string(labelName);
        encoder.put_byte((byte) 1); // Assume the procedure index is 1
        Result<byte[]> resp = sess.callProcedureRaw(graphId, bytes);
        if (!resp.isOk()) {
            throw new RuntimeException("Fail to call procedure" + resp.getStatusMessage());
        }
        Decoder decoder = new Decoder(resp.getValue());
        int count = decoder.get_int();
        System.out.println("Count of vertices with label " + labelName + " is " + count); // should be 4
    }

    public static void startServiceOnGraph(Session sess, String graphId) {
        Result<StartServiceResponse> resp = sess.startService(new StartServiceRequest().graphId(graphId));
        if (!resp.isOk()) {
            throw new RuntimeException("Fail to start service" + resp.getStatusMessage());
        }
    }

    public static void main(String[] args) {
        Driver driver = Driver.connect();
        Session sess = driver.session();
        startServiceOnGraph(sess, "1"); // Procedure is only runnable after service has been switched to graph 1
        callProcedure(sess, "1", "count_vertices", "person"); // count how many vertices are labeled with person.
        return ;
    }
}
```


```python
from gs_interactive.client.driver import Driver
from gs_interactive.models import *
from gs_interactive.client.utils import *

driver = Driver() # connecting to Interactive service, assuming environment variables like INTERACTIVE_ADMIN_ENDPOINT have been correctly exported.
sess = driver.session() # create the session

# Use a encoder to encode input request
encoder = Encoder()
encoder.put_string("person") # input label name
encoder.put_byte(1) # procedure id 1
resp = sess.call_procedure_raw(graph_id="1", params=encoder.get_bytes())
assert resp.is_ok()
decoder = Decoder(resp.value)
num = decoder.get_int()
print(f"vertices num: {num}")
```

## Programming Interface

To create an efficient procedure that meets your needs, it's essential to understand the programming interface and the Interactive storage interface. We recommend reading the [source code](https://github.com/alibaba/GraphScope/tree/main/flex) of Interactive, and you can also access the generated API documentation [here](https://graphscope.io/docs/reference/flex/).



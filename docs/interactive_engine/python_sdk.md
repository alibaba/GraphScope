# Insight Python SDK Guild

The Python SDK guild is designed to assist developers in integrating the Interactive service into their Python applications. This SDK allows users to seamlessly connect to Insight (with Groot storage) Http Service and harness its powerful features for graph management and query execution.

## Requirements.

Python >= 3.8

## Installation & Usage

```{note}
If you want to isolate the installation from your local Python environment, you might consider using a virtual environment [virtualenv](https://virtualenv.pypa.io/) to create a new one."
```

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
python3 setup.py install --user
```
(or `sudo python3 setup.py install` to install the package for all users)

Then import the package:
```python
import gs_interactive
```

### Tests

Execute `pytest` to run the tests.

## Getting Started

First, install and deploy Groot by following the instructions in [Deploy Groot](../storage_engine/groot.md#deploy-groot). This process will start the HTTP service. The HTTP service endpoint can be configured using the `frontend.httpPort` option, which defaults to port 8080.
Then you can connect to Interactive service with Interactive SDK, with following environment variables declared.

```bash
############################################################################################
    export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:8080
############################################################################################
```

```{note}
If you have customized the ports when deploying Interactive, remember to replace the default ports with your customized ports.
```

### Create a new graph

To create a new graph, user need to specify the name, description, vertex types and edges types.
For the detail data model of the graph, please refer to [Data Model](../../data_model). 

In this example, we will create a simple graph with only one vertex type `persson`, and one edge type named `knows`.

```python
def create_graph(sess : Session):
    # Define the graph schema via a python dict.
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
    create_graph_request = CreateGraphRequest.from_dict(test_graph_def)
    resp = sess.create_graph(create_graph_request)
    assert resp.is_ok()
    return resp.get_value().graph_id

driver = Driver()
sess = driver.session()

graph_id = create_graph(sess)
print("Created graph, id is ", graph_id)
```

In the aforementioned example, a graph named `test_graph` is defined using a python dictionaly. You can also define the graph using the programmatic interface provided by [CreateGraphRequest](./CreateGraphRequest.md). Upon calling the `createGraph` method, a string representing the unique identifier of the graph is returned.

````{note}
You might observe that we define the graph schema in YAML with `gsctl`, but switch to using `dict` in Python code. You may encounter challenges when converting between different formats.
However, converting `YAML` to a Python `dict` is quite convenient.

First, install pyYAML

```bash
pip3 install pyYAML
```

Then use pyYAML to convert the YAML string to a Python dict

```python
import yaml

yaml_string = """
...
"""

python_dict = yaml.safe_load(yaml_string)

print(python_dict)
```

Afterwards, you can create a `CreateGraphRequest` from the Python dict.
````



### Import data to the graph

After a new graph is created, you may want to import data into the newly created graph. Currently, real-time data writing is supported via the HTTP service. For details on offline data loading, please refer to [Data Import](../storage_engine/groot.md#data-import). Offline data loading will be supported via the HTTP service soon.

For example, you can insert vertices and edges as follows:

```python
# Add vertices and edges
vertex_request = [
    VertexRequest(
        label="person",
        primary_key_values= [
            ModelProperty(name="id", value=1),
        ],
        properties=[
            ModelProperty(name="name", value="Alice"),
            ModelProperty(name="age", value=20),
        ],
    ),
    VertexRequest(
        label="person",
        primary_key_values= [
            ModelProperty(name="id", value=8),
        ],            
        properties=[
            ModelProperty(name="name", value="mike"),
            ModelProperty(name="age", value=1),
        ],
    ),
]
edge_request = [
    EdgeRequest(
        src_label="person",
        dst_label="person",
        edge_label="knows",
        src_primary_key_values=[ModelProperty(name="id", value=8)],
        dst_primary_key_values=[ModelProperty(name="id", value=1)],
        properties=[ModelProperty(name="weight", value=7)],
    ),
]
api_response = sess.add_vertex(graph_id, vertex_edge_request=VertexEdgeRequest(vertex_request=vertex_request, edge_request=edge_request))

# the response will return the snapshot_id after the realtime write.
snapshot_id = ast.literal_eval(api_response.get_value()).get("snapshot_id")
# get the snapshot status to check if the written data is available for querying
snapshot_status =  sess.get_snapshot_status(graph_id, snapshot_id)

```

### Modify the graph schema
You may want to modify the graph schema to accommodate new types of vertices or edges, add properties to existing types, or delete existing types as needed.

For example, you can create new vertex and edge types as follows:

```python
# create new vertex type
create_vertex_type = CreateVertexType(
    type_name="new_person",
    properties=[
        CreatePropertyMeta(
            property_name="id",
            property_type=GSDataType.from_dict({"primitive_type": "DT_SIGNED_INT64"}),
        ),
        CreatePropertyMeta(
            property_name="name",
            property_type=GSDataType.from_dict({"string": {"long_text": ""}}),
        ),
    ],
    primary_keys=["id"],
)
api_response = sess.create_vertex_type(graph_id, create_vertex_type)

# create new edge type
create_edge_type = CreateEdgeType(
    type_name="new_knows",
    vertex_type_pair_relations=[
        BaseEdgeTypeVertexTypePairRelationsInner(
            source_vertex="new_person",
            destination_vertex="new_person",
            relation="MANY_TO_MANY",
        )
    ],
    properties=[
        CreatePropertyMeta(
            property_name="weight",
            property_type=GSDataType.from_dict({"primitive_type": "DT_DOUBLE"}),
        )
    ],
)
api_response = sess.create_edge_type(graph_id, create_edge_type)
```

### Delete the graph

Finally, we can delete the graph, as follows:

```python
resp = sess.delete_graph(graph_id)
assert resp.is_ok()
print("delete graph res: ", resp)
```

For the full example on the Insight engine (with Groot storage), please refer to [Python SDK Example Insight](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/sdk/examples/python/insight_example.py).

For the full documentation for python sdk reference, please refer to [Python SDK Reference](./python_sdk_ref.md).
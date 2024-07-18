import gs_interactive
from gs_interactive.models.vertex_request import VertexRequest
from gs_interactive.models.edge_request import EdgeRequest
from gs_interactive.models.property_array import PropertyArray
from gs_interactive.models.model_property import ModelProperty
from gs_interactive.rest import ApiException
import argparse

# Defining the host is optional and defaults to {INTERACTIVE_ENDPOINT}
# See configuration.py for a list of all supported configuration parameters.
configuration = gs_interactive.Configuration(host="INTERACTIVE_ENDPOINT")

class VertexSDK:
    @staticmethod
    def add():
        with gs_interactive.ApiClient(configuration) as api_client:
            api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
            graph_id = "1"  # str |
            vertex_request = [
                VertexRequest(
                    label="person",
                    primary_key_value=8,
                    properties=PropertyArray(
                        properties=[
                            ModelProperty(name="name", type="string", value="mike"),
                            ModelProperty(name="age", type="integer", value=1),
                        ]
                    ),
                ),
                VertexRequest(
                    label="person",
                    primary_key_value=7,
                    properties=PropertyArray(
                        properties=[
                            ModelProperty(name="name", type="string", value="lisa"),
                            ModelProperty(name="age", type="integer", value=2),
                        ]
                    ),
                ),
            ]
            edge_request = [
                EdgeRequest(
                    src_label="person",
                    dst_label="software",
                    edge_label="created",
                    src_primary_key_value=8,
                    dst_primary_key_value=5,
                    properties=[ModelProperty(name="weight", value=7)],
                ),
                EdgeRequest(
                    src_label="person",
                    dst_label="software",
                    edge_label="created",
                    src_primary_key_value=8,
                    dst_primary_key_value=3,
                    properties=[ModelProperty(name="weight", value=5)],
                ),
            ]
            try:
                api_response = api_instance.add_vertex(
                    graph_id, vertex_request=vertex_request, edge_request=edge_request
                )
                print(
                    "The response of GraphServiceVertexManagementApi->add_vertex:",
                    api_response,
                )
            except Exception as e:
                print(
                    "Exception when calling GraphServiceVertexManagementApi->add_vertex: %s\n"
                    % e
                )
    @staticmethod
    def update():
        with gs_interactive.ApiClient(configuration) as api_client:
            api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
            graph_id = "1"  # str | The id of the graph
            age_property = ModelProperty(name="age", type="integer", value=24)
            name_property = ModelProperty(name="name", type="string", value="Cindy")
            properties = PropertyArray(properties=[name_property, age_property])
            vertex_request = VertexRequest(
                label="person", primary_key_value=1, properties=properties
            )
            try:
                api_response = api_instance.update_vertex(
                    graph_id, vertex_request=vertex_request
                )
                print("The response of update_vertex:", api_response)
            except Exception as e:
                print("Exception when calling update_vertex: %s\n" % e)
    @staticmethod
    def get():
        with gs_interactive.ApiClient(configuration) as api_client:
            api_instance = gs_interactive.GraphServiceVertexManagementApi(api_client)
            graph_id = "1"  # str | The id of the graph
            label = "person"  # str | The label name of querying vertex.
            primary_key_value = 8  # object | The primary key value of querying vertex.
            try:
                api_response = api_instance.get_vertex(
                    graph_id, label, primary_key_value
                )
                print("The response of get_vertex:", api_response)
            except Exception as e:
                print("Exception when calling get_vertex: %s" % e)


class EdgeSDK:
    @staticmethod
    def update_edge():
        with gs_interactive.ApiClient(configuration) as api_client:
            api_instance = gs_interactive.GraphServiceEdgeManagementApi(api_client)
            graph_id = "1"
            properties = [ModelProperty(name="weight", value=3)]
            edge_request = EdgeRequest(
                src_label="person",
                dst_label="software",
                edge_label="created",
                src_primary_key_value=1,
                dst_primary_key_value=3,
                properties=properties,
            )
            try:
                api_response = api_instance.update_edge(
                    graph_id, edge_request=edge_request
                )
                print("The response of GraphServiceEdgeManagementApi->update_edge:", api_response)
            except Exception as e:
                print(
                    "Exception when calling GraphServiceEdgeManagementApi->update_edge: %s\n"
                    % e
                )
    @staticmethod
    def get_edge():
        with gs_interactive.ApiClient(configuration) as api_client:
            api_instance = gs_interactive.GraphServiceEdgeManagementApi(api_client)
            graph_id = "1"
            src_label = "person"

            dst_label = "software"
            edge_label = "created"
            src_primary_key_value = 8
            dst_primary_key_value = 5
            try:
                api_response = api_instance.get_edge(
                    graph_id,
                    edge_label,
                    src_label,
                    src_primary_key_value,
                    dst_label,
                    dst_primary_key_value,
                )
                print("The response of GraphServiceEdgeManagementApi->get_edge:", api_response)
            except ApiException as e:
                print(
                    "Exception when calling GraphServiceEdgeManagementApi->get_edge: %s\n"
                    % e
                )
    @staticmethod
    def add_edge():
        with gs_interactive.ApiClient(configuration) as api_client:
            # Create an instance of the API class
            api_instance = gs_interactive.GraphServiceEdgeManagementApi(api_client)
            graph_id = "1"  # str |
            edge_request = [
                EdgeRequest(
                    src_label="person",
                    dst_label="software",
                    edge_label="created",
                    src_primary_key_value=1,
                    dst_primary_key_value=5,
                    properties=[ModelProperty(name="weight", value=9.123)],
                ),
                EdgeRequest(
                    src_label="person",
                    dst_label="software",
                    edge_label="created",
                    src_primary_key_value=2,
                    dst_primary_key_value=5,
                    properties=[ModelProperty(name="weight", value=3.233)],
                ),
            ]
            try:
                api_response = api_instance.add_edge(graph_id, edge_request)
                print("The response of GraphServiceEdgeManagementApi->add_edge:", api_response)
            except Exception as e:
                print(
                    "Exception when calling GraphServiceEdgeManagementApi->add_edge: %s\n"
                    % e
                )


func_map = {
    'vertex': {
        'get': VertexSDK.get,
        'update': VertexSDK.update,
        'add': VertexSDK.add,
    },
    'edge': {
        'get': EdgeSDK.get_edge,
        'update': EdgeSDK.update_edge,
        'add': EdgeSDK.add_edge,
    }
}


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Interactive vertex and edge management example")
    parser.add_argument("--func", "-f", type=str, help="The function to call", required=True, choices=["get", "update", "add"])
    parser.add_argument("--type", "-t", type=str, help="The type of the operation", required=True, choices=["vertex", "edge"])
    args = parser.parse_args()
    try:
        operation = func_map[args.type][args.func]
        operation()
    except KeyError:
        print("Invalid operation type or function")


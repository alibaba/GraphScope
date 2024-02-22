import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception
from gs_flex_coordinator.models.edge_type import EdgeType  # noqa: E501
from gs_flex_coordinator.models.graph import Graph  # noqa: E501
from gs_flex_coordinator.models.model_schema import ModelSchema  # noqa: E501
from gs_flex_coordinator.models.vertex_type import VertexType  # noqa: E501
from gs_flex_coordinator import util


@handle_api_exception()
def create_edge_type(graph_name, edge_type):  # noqa: E501
    """create_edge_type

    Create a edge type # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param edge_type: 
    :type edge_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        edge_type = EdgeType.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.create_edge_type(graph_name, edge_type)


@handle_api_exception()
def create_graph(graph):  # noqa: E501
    """create_graph

    Create a new graph # noqa: E501

    :param graph: 
    :type graph: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        graph = Graph.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.create_graph(graph)


@handle_api_exception()
def create_vertex_type(graph_name, vertex_type):  # noqa: E501
    """create_vertex_type

    Create a vertex type # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param vertex_type: 
    :type vertex_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        vertex_type = VertexType.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.create_vertex_type(graph_name, vertex_type)


@handle_api_exception()
def delete_edge_type(graph_name, type_name, source_vertex_type, destination_vertex_type):  # noqa: E501
    """delete_edge_type

    Delete a edge type by name # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param type_name: 
    :type type_name: str
    :param source_vertex_type: 
    :type source_vertex_type: str
    :param destination_vertex_type: 
    :type destination_vertex_type: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.delete_edge_type(
        graph_name, type_name, source_vertex_type, destination_vertex_type
    )


@handle_api_exception()
def delete_graph(graph_name):  # noqa: E501
    """delete_graph

    Delete a graph by name # noqa: E501

    :param graph_name: 
    :type graph_name: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.delete_graph_by_name(graph_name)


@handle_api_exception()
def delete_vertex_type(graph_name, type_name):  # noqa: E501
    """delete_vertex_type

    Delete a vertex type by name # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param type_name: 
    :type type_name: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.delete_vertex_type(graph_name, type_name)


@handle_api_exception()
def get_schema(graph_name):  # noqa: E501
    """get_schema

    Get graph schema by name # noqa: E501

    :param graph_name: 
    :type graph_name: str

    :rtype: Union[ModelSchema, Tuple[ModelSchema, int], Tuple[ModelSchema, int, Dict[str, str]]
    """
    return client_wrapper.get_schema_by_name(graph_name)


@handle_api_exception()
def list_graphs():  # noqa: E501
    """list_graphs

    List all graphs # noqa: E501


    :rtype: Union[List[Graph], Tuple[List[Graph], int], Tuple[List[Graph], int, Dict[str, str]]
    """
    return client_wrapper.list_graphs()

import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception
from gs_flex_coordinator.models.data_source import DataSource  # noqa: E501
from gs_flex_coordinator.models.edge_data_source import EdgeDataSource  # noqa: E501
from gs_flex_coordinator.models.vertex_data_source import VertexDataSource  # noqa: E501
from gs_flex_coordinator import util


@handle_api_exception()
def bind_edge_datasource(graph_name, edge_data_source):  # noqa: E501
    """bind_edge_datasource

    Bind data source on edge type # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param edge_data_source: 
    :type edge_data_source: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        edge_data_source = EdgeDataSource.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.bind_edge_datasource(graph_name, edge_data_source)


@handle_api_exception()
def bind_vertex_datasource(graph_name, vertex_data_source):  # noqa: E501
    """bind_vertex_datasource

    Bind data source on vertex type # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param vertex_data_source: 
    :type vertex_data_source: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        vertex_data_source = VertexDataSource.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.bind_vertex_datasource(graph_name, vertex_data_source)


@handle_api_exception()
def get_datasource(graph_name):  # noqa: E501
    """get_datasource

    List data source on graph # noqa: E501

    :param graph_name: 
    :type graph_name: str

    :rtype: Union[DataSource, Tuple[DataSource, int], Tuple[DataSource, int, Dict[str, str]]
    """
    return client_wrapper.get_datasource(graph_name)


@handle_api_exception()
def get_edge_datasource(graph_name, type_name, source_vertex_type, destination_vertex_type):  # noqa: E501
    """get_edge_datasource

    Get edge data source # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param type_name: 
    :type type_name: str
    :param source_vertex_type: 
    :type source_vertex_type: str
    :param destination_vertex_type: 
    :type destination_vertex_type: str

    :rtype: Union[EdgeDataSource, Tuple[EdgeDataSource, int], Tuple[EdgeDataSource, int, Dict[str, str]]
    """
    return client_wrapper.get_edge_datasource(
        graph_name,
        type_name,
        source_vertex_type,
        destination_vertex_type
    )


@handle_api_exception()
def get_vertex_datasource(graph_name, type_name):  # noqa: E501
    """get_vertex_datasource

    Get vertex data source # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param type_name: 
    :type type_name: str

    :rtype: Union[VertexDataSource, Tuple[VertexDataSource, int], Tuple[VertexDataSource, int, Dict[str, str]]
    """
    return client_wrapper.get_vertex_datasource(graph_name, type_name)


@handle_api_exception()
def import_datasource(graph_name, data_source):  # noqa: E501
    """import_datasource

    Import data source in batch # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param data_source: 
    :type data_source: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        data_source = DataSource.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.import_datasource(graph_name, data_source)


@handle_api_exception()
def unbind_edge_datasource(graph_name, type_name, source_vertex_type, destination_vertex_type):  # noqa: E501
    """unbind_edge_datasource

    Unbind datasource on an edge type # noqa: E501

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
    return client_wrapper.unbind_edge_datasource(
        graph_name,
        type_name,
        source_vertex_type,
        destination_vertex_type
    )


@handle_api_exception()
def unbind_vertex_datasource(graph_name, type_name):  # noqa: E501
    """unbind_vertex_datasource

    Unbind datasource on a vertex type # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param type_name: 
    :type type_name: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.unbind_vertex_datasource(graph_name, type_name)

import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gscoordinator.flex.models.create_edge_type import CreateEdgeType  # noqa: E501
from gscoordinator.flex.models.create_graph_request import CreateGraphRequest  # noqa: E501
from gscoordinator.flex.models.create_graph_response import CreateGraphResponse  # noqa: E501
from gscoordinator.flex.models.create_graph_schema_request import CreateGraphSchemaRequest  # noqa: E501
from gscoordinator.flex.models.create_vertex_type import CreateVertexType  # noqa: E501
from gscoordinator.flex.models.error import Error  # noqa: E501
from gscoordinator.flex.models.get_graph_response import GetGraphResponse  # noqa: E501
from gscoordinator.flex.models.get_graph_schema_response import GetGraphSchemaResponse  # noqa: E501
from gscoordinator.flex import util

from gscoordinator.flex.core import client_wrapper
from gscoordinator.flex.core import handle_api_exception


def create_edge_type(graph_id, create_edge_type=None):  # noqa: E501
    """create_edge_type

    Create a edge type # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param create_edge_type: 
    :type create_edge_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_edge_type = CreateEdgeType.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


@handle_api_exception()
def create_graph(create_graph_request):  # noqa: E501
    """create_graph

    Create a new graph # noqa: E501

    :param create_graph_request: 
    :type create_graph_request: dict | bytes

    :rtype: Union[CreateGraphResponse, Tuple[CreateGraphResponse, int], Tuple[CreateGraphResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_graph_request = CreateGraphRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.create_graph(create_graph_request)


def create_vertex_type(graph_id, create_vertex_type):  # noqa: E501
    """create_vertex_type

    Create a vertex type # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param create_vertex_type: 
    :type create_vertex_type: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_vertex_type = CreateVertexType.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def delete_edge_type_by_name(graph_id, type_name, source_vertex_type, destination_vertex_type):  # noqa: E501
    """delete_edge_type_by_name

    Delete edge type by name # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param type_name: 
    :type type_name: str
    :param source_vertex_type: 
    :type source_vertex_type: str
    :param destination_vertex_type: 
    :type destination_vertex_type: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return 'do some magic!'


@handle_api_exception()
def delete_graph_by_id(graph_id):  # noqa: E501
    """delete_graph_by_id

    Delete graph by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.delete_graph_by_id(graph_id)


def delete_vertex_type_by_name(graph_id, type_name):  # noqa: E501
    """delete_vertex_type_by_name

    Delete vertex type by name # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param type_name: 
    :type type_name: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return 'do some magic!'


@handle_api_exception()
def get_graph_by_id(graph_id):  # noqa: E501
    """get_graph_by_id

    Get graph by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str

    :rtype: Union[GetGraphResponse, Tuple[GetGraphResponse, int], Tuple[GetGraphResponse, int, Dict[str, str]]
    """
    return client_wrapper.get_graph_by_id(graph_id)


@handle_api_exception()
def get_schema_by_id(graph_id):  # noqa: E501
    """get_schema_by_id

    Get graph schema by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str

    :rtype: Union[GetGraphSchemaResponse, Tuple[GetGraphSchemaResponse, int], Tuple[GetGraphSchemaResponse, int, Dict[str, str]]
    """
    return client_wrapper.get_schema_by_id(graph_id)


def import_schema_by_id(graph_id, create_graph_schema_request):  # noqa: E501
    """import_schema_by_id

    Import graph schema # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param create_graph_schema_request: 
    :type create_graph_schema_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_graph_schema_request = CreateGraphSchemaRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


@handle_api_exception()
def list_graphs():  # noqa: E501
    """list_graphs

    List all graphs # noqa: E501


    :rtype: Union[List[GetGraphResponse], Tuple[List[GetGraphResponse], int], Tuple[List[GetGraphResponse], int, Dict[str, str]]
    """
    return client_wrapper.list_graphs()

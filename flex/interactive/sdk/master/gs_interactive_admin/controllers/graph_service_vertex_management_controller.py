import logging
from typing import Dict
from typing import Tuple
from typing import Union

import connexion

from gs_interactive_admin import util
from gs_interactive_admin.models.api_response_with_code import (  # noqa: E501
    APIResponseWithCode,
)
from gs_interactive_admin.models.delete_vertex_request import (  # noqa: E501
    DeleteVertexRequest,
)
from gs_interactive_admin.models.vertex_data import VertexData  # noqa: E501
from gs_interactive_admin.models.vertex_edge_request import (  # noqa: E501
    VertexEdgeRequest,
)

logger = logging.getLogger("interactive")


def add_vertex(graph_id, vertex_edge_request):  # noqa: E501
    """Add vertex (and edge) to the graph

    Add the provided vertex (and edge) to the specified graph.  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param vertex_edge_request:
    :type vertex_edge_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        vertex_edge_request = VertexEdgeRequest.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"


def delete_vertex(graph_id, delete_vertex_request):  # noqa: E501
    """Remove vertex from the graph

    Remove the vertex from the specified graph.  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param delete_vertex_request: The label and primary key values of the vertex to be deleted.
    :type delete_vertex_request: list | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        delete_vertex_request = [  # noqa: F841
            DeleteVertexRequest.from_dict(d) for d in connexion.request.get_json()
        ]  # noqa: E501
    return "do some magic!"


def get_vertex(graph_id, label, primary_key_value):  # noqa: E501
    """Get the vertex&#39;s properties with vertex primary key.

    Get the properties for the specified vertex. example: &#x60;&#x60;&#x60;http GET /endpoint?param1&#x3D;value1&amp;param2&#x3D;value2 HTTP/1.1 Host: example.com &#x60;&#x60;&#x60;  # noqa: E501

    :param graph_id: The id of the graph
    :type graph_id: str
    :param label: The label name of querying vertex.
    :type label: str
    :param primary_key_value: The primary key value of querying vertex.
    :type primary_key_value: dict | bytes

    :rtype: Union[VertexData, Tuple[VertexData, int], Tuple[VertexData, int, Dict[str, str]]
    """
    return "do some magic!"


def update_vertex(graph_id, vertex_edge_request):  # noqa: E501
    """Update vertex&#39;s property

    Update the vertex with the provided properties to the specified graph.  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param vertex_edge_request:
    :type vertex_edge_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        vertex_edge_request = VertexEdgeRequest.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"

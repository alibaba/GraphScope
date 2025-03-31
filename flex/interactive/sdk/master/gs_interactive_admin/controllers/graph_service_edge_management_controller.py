import logging
from typing import Dict
from typing import Tuple
from typing import Union

import connexion

from gs_interactive_admin import util
from gs_interactive_admin.models.api_response_with_code import (  # noqa: E501
    APIResponseWithCode,
)
from gs_interactive_admin.models.delete_edge_request import (  # noqa: E501
    DeleteEdgeRequest,
)
from gs_interactive_admin.models.edge_data import EdgeData  # noqa: E501
from gs_interactive_admin.models.edge_request import EdgeRequest  # noqa: E501

logger = logging.getLogger("interactive")


def add_edge(graph_id, edge_request):  # noqa: E501
    """Add edge to the graph

    Add the edge to graph.  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param edge_request:
    :type edge_request: list | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        edge_request = [  # noqa: F841
            EdgeRequest.from_dict(d) for d in connexion.request.get_json()
        ]  # noqa: E501
    return "do some magic!"


def delete_edge(graph_id, delete_edge_request):  # noqa: E501
    """Remove edge from the graph

    Remove the edge from current graph.  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param delete_edge_request: The label and primary key values of the src and dst vertices, and the edge label.
    :type delete_edge_request: list | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        delete_edge_request = [  # noqa: F841
            DeleteEdgeRequest.from_dict(d) for d in connexion.request.get_json()
        ]  # noqa: E501
    return "do some magic!"


def get_edge(
    graph_id,
    edge_label,
    src_label,
    src_primary_key_value,
    dst_label,
    dst_primary_key_value,
):  # noqa: E501
    """Get the edge&#39;s properties with src and dst vertex primary keys.

    Get the properties for the specified vertex.  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param edge_label: The label name of querying edge.
    :type edge_label: str
    :param src_label: The label name of src vertex.
    :type src_label: str
    :param src_primary_key_value: The primary key value of src vertex.
    :type src_primary_key_value: dict | bytes
    :param dst_label: The label name of dst vertex.
    :type dst_label: str
    :param dst_primary_key_value: The value of dst vertex&#39;s primary key
    :type dst_primary_key_value: dict | bytes

    :rtype: Union[EdgeData, Tuple[EdgeData, int], Tuple[EdgeData, int, Dict[str, str]]
    """
    return "do some magic!"


def update_edge(graph_id, edge_request):  # noqa: E501
    """Update edge&#39;s property

    Update the edge on the running graph.  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param edge_request:
    :type edge_request: list | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        edge_request = [  # noqa: F841
            EdgeRequest.from_dict(d) for d in connexion.request.get_json()
        ]  # noqa: E501
    return "do some magic!"

import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception
from gs_flex_coordinator.models.groot_graph import GrootGraph  # noqa: E501
from gs_flex_coordinator.models.groot_schema import GrootSchema  # noqa: E501
from gs_flex_coordinator.models.schema_mapping import SchemaMapping  # noqa: E501
from gs_flex_coordinator import util


@handle_api_exception()
def get_groot_schema(graph_name):  # noqa: E501
    """get_groot_schema

    Get graph schema by name # noqa: E501

    :param graph_name: 
    :type graph_name: str

    :rtype: Union[GrootSchema, Tuple[GrootSchema, int], Tuple[GrootSchema, int, Dict[str, str]]
    """
    return 'do some magic!'


@handle_api_exception()
def import_schema(graph_name, groot_schema):  # noqa: E501
    """import_schema

    Import schema to groot graph # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param groot_schema: 
    :type groot_schema: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        groot_schema = GrootSchema.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


@handle_api_exception()
def list_groot_graph():  # noqa: E501
    """list_groot_graph

    list groot graph # noqa: E501


    :rtype: Union[List[GrootGraph], Tuple[List[GrootGraph], int], Tuple[List[GrootGraph], int, Dict[str, str]]
    """
    return 'do some magic!'

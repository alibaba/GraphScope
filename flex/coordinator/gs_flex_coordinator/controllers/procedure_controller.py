import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception
from gs_flex_coordinator.models.procedure import Procedure  # noqa: E501
from gs_flex_coordinator import util


@handle_api_exception()
def create_procedure(graph_name, procedure):  # noqa: E501
    """create_procedure

    Create a new stored procedure on a certain graph # noqa: E501

    :param graph_name:
    :type graph_name: str
    :param procedure:
    :type procedure: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        procedure = Procedure.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.create_procedure(graph_name, procedure)


@handle_api_exception()
def delete_procedure(graph_name, procedure_name):  # noqa: E501
    """delete_procedure

    Delete a stored procedure on a certain graph # noqa: E501

    :param graph_name:
    :type graph_name: str
    :param procedure_name:
    :type procedure_name: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.delete_procedure_by_name(graph_name, procedure_name)


@handle_api_exception()
def list_procedures():  # noqa: E501
    """list_procedures

    List all the stored procedures # noqa: E501


    :rtype: Union[List[Procedure], Tuple[List[Procedure], int], Tuple[List[Procedure], int, Dict[str, str]]
    """
    return client_wrapper.list_procedures(graph_name=None)


@handle_api_exception()
def list_procedures_by_graph(graph_name):  # noqa: E501
    """list_procedures_by_graph

    List stored procedures on a certain graph # noqa: E501

    :param graph_name:
    :type graph_name: str

    :rtype: Union[List[Procedure], Tuple[List[Procedure], int], Tuple[List[Procedure], int, Dict[str, str]]
    """
    return client_wrapper.list_procedures(graph_name=graph_name)


@handle_api_exception()
def update_procedure(graph_name, procedure_name, procedure=None):  # noqa: E501
    """update_procedure

    Update stored procedure on a certain graph # noqa: E501

    :param graph_name:
    :type graph_name: str
    :param procedure_name:
    :type procedure_name: str
    :param procedure:
    :type procedure: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        procedure = Procedure.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.update_procedure(graph_name, procedure_name, procedure)

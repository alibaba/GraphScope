import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.models.create_procedure_request import CreateProcedureRequest  # noqa: E501
from gs_flex_coordinator.models.create_procedure_response import CreateProcedureResponse  # noqa: E501
from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.get_procedure_response import GetProcedureResponse  # noqa: E501
from gs_flex_coordinator.models.update_procedure_request import UpdateProcedureRequest  # noqa: E501
from gs_flex_coordinator import util

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception


@handle_api_exception()
def create_procedure(graph_id, create_procedure_request):  # noqa: E501
    """create_procedure

    Create a new stored procedure on a certain graph # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param create_procedure_request: 
    :type create_procedure_request: dict | bytes

    :rtype: Union[CreateProcedureResponse, Tuple[CreateProcedureResponse, int], Tuple[CreateProcedureResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_procedure_request = CreateProcedureRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.create_procedure(graph_id, create_procedure_request)


@handle_api_exception()
def delete_procedure_by_id(graph_id, procedure_id):  # noqa: E501
    """delete_procedure_by_id

    Delete a stored procedure by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param procedure_id: 
    :type procedure_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.delete_procedure_by_id(graph_id, procedure_id)


@handle_api_exception()
def get_procedure_by_id(graph_id, procedure_id):  # noqa: E501
    """get_procedure_by_id

    Get a stored procedure by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param procedure_id: 
    :type procedure_id: str

    :rtype: Union[GetProcedureResponse, Tuple[GetProcedureResponse, int], Tuple[GetProcedureResponse, int, Dict[str, str]]
    """
    return client_wrapper.get_procedure_by_id(graph_id, procedure_id)


@handle_api_exception()
def list_procedures(graph_id):  # noqa: E501
    """list_procedures

    List all stored procedures on a certain graph # noqa: E501

    :param graph_id: 
    :type graph_id: str

    :rtype: Union[List[GetProcedureResponse], Tuple[List[GetProcedureResponse], int], Tuple[List[GetProcedureResponse], int, Dict[str, str]]
    """
    return client_wrapper.list_procedures(graph_id)


@handle_api_exception()
def update_procedure_by_id(graph_id, procedure_id, update_procedure_request=None):  # noqa: E501
    """update_procedure_by_id

    Update a stored procedure by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param procedure_id: 
    :type procedure_id: str
    :param update_procedure_request: 
    :type update_procedure_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        update_procedure_request = UpdateProcedureRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.update_procedure_by_id(graph_id, procedure_id, update_procedure_request)

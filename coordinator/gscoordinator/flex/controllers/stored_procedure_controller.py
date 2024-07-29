import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gscoordinator.flex.models.create_stored_proc_request import CreateStoredProcRequest  # noqa: E501
from gscoordinator.flex.models.create_stored_proc_response import CreateStoredProcResponse  # noqa: E501
from gscoordinator.flex.models.error import Error  # noqa: E501
from gscoordinator.flex.models.get_stored_proc_response import GetStoredProcResponse  # noqa: E501
from gscoordinator.flex.models.update_stored_proc_request import UpdateStoredProcRequest  # noqa: E501
from gscoordinator.flex import util

from gscoordinator.flex.core import client_wrapper
from gscoordinator.flex.core import handle_api_exception


@handle_api_exception()
def create_stored_procedure(graph_id, create_stored_proc_request):  # noqa: E501
    """create_stored_procedure

    Create a new stored procedure on a certain graph # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param create_stored_proc_request: 
    :type create_stored_proc_request: dict | bytes

    :rtype: Union[CreateStoredProcResponse, Tuple[CreateStoredProcResponse, int], Tuple[CreateStoredProcResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_stored_proc_request = CreateStoredProcRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.create_stored_procedure(graph_id, create_stored_proc_request)


@handle_api_exception()
def delete_stored_procedure_by_id(graph_id, stored_procedure_id):  # noqa: E501
    """delete_stored_procedure_by_id

    Delete a stored procedure by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param stored_procedure_id: 
    :type stored_procedure_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.delete_stored_procedure_by_id(graph_id, stored_procedure_id)


@handle_api_exception()
def get_stored_procedure_by_id(graph_id, stored_procedure_id):  # noqa: E501
    """get_stored_procedure_by_id

    Get a stored procedure by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param stored_procedure_id: 
    :type stored_procedure_id: str

    :rtype: Union[GetStoredProcResponse, Tuple[GetStoredProcResponse, int], Tuple[GetStoredProcResponse, int, Dict[str, str]]
    """
    return client_wrapper.get_stored_procedure_by_id(graph_id, stored_procedure_id)


@handle_api_exception()
def list_stored_procedures(graph_id):  # noqa: E501
    """list_stored_procedures

    List all stored procedures on a certain graph # noqa: E501

    :param graph_id: 
    :type graph_id: str

    :rtype: Union[List[GetStoredProcResponse], Tuple[List[GetStoredProcResponse], int], Tuple[List[GetStoredProcResponse], int, Dict[str, str]]
    """
    return client_wrapper.list_stored_procedures(graph_id)


@handle_api_exception()
def update_stored_procedure_by_id(graph_id, stored_procedure_id, update_stored_proc_request=None):  # noqa: E501
    """update_stored_procedure_by_id

    Update a stored procedure by ID # noqa: E501

    :param graph_id: 
    :type graph_id: str
    :param stored_procedure_id: 
    :type stored_procedure_id: str
    :param update_stored_proc_request: 
    :type update_stored_proc_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        update_stored_proc_request = UpdateStoredProcRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.update_stored_procedure_by_id(graph_id, stored_procedure_id, update_stored_proc_request)

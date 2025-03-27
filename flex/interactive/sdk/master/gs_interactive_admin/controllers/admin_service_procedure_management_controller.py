import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_interactive_admin.models.api_response_with_code import (
    APIResponseWithCode,
)  # noqa: E501
from gs_interactive_admin.models.create_procedure_request import (
    CreateProcedureRequest,
)  # noqa: E501
from gs_interactive_admin.models.create_procedure_response import (
    CreateProcedureResponse,
)  # noqa: E501
from gs_interactive_admin.models.get_procedure_response import (
    GetProcedureResponse,
)  # noqa: E501
from gs_interactive_admin.models.update_procedure_request import (
    UpdateProcedureRequest,
)  # noqa: E501
from gs_interactive_admin import util

from gs_interactive_admin.core.procedure.procedure_manager import ProcedureManager, get_procedure_manager


def create_procedure(graph_id, create_procedure_request):  # noqa: E501
    """create_procedure

    Create a new procedure on a graph # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_procedure_request:
    :type create_procedure_request: dict | bytes

    :rtype: Union[CreateProcedureResponse, Tuple[CreateProcedureResponse, int], Tuple[CreateProcedureResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_procedure_request = CreateProcedureRequest.from_dict(
            connexion.request.get_json()
        )
        return get_procedure_manager().create_procedure(
            graph_id=graph_id, create_procedure_request=create_procedure_request
        )
    else:
        raise RuntimeError("Invalid request")


def delete_procedure(graph_id, procedure_id):  # noqa: E501
    """delete_procedure

    Delete a procedure on a graph by id # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param procedure_id:
    :type procedure_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        return get_procedure_manager().delete_procedure(
            graph_id=graph_id, procedure_id=procedure_id
        )
    else:
        raise RuntimeError("Invalid request")


def get_procedure(graph_id, procedure_id):  # noqa: E501
    """get_procedure

    Get a procedure by id # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param procedure_id:
    :type procedure_id: str

    :rtype: Union[GetProcedureResponse, Tuple[GetProcedureResponse, int], Tuple[GetProcedureResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        return get_procedure_manager().get_procedure(
            graph_id=graph_id, procedure_id=procedure_id
        )
    else:
        raise RuntimeError("Invalid request")


def list_procedures(graph_id):  # noqa: E501
    """list_procedures

    List all procedures # noqa: E501

    :param graph_id:
    :type graph_id: str

    :rtype: Union[List[GetProcedureResponse], Tuple[List[GetProcedureResponse], int], Tuple[List[GetProcedureResponse], int, Dict[str, str]]
    """
    if connexion.request.is_json:
        return get_procedure_manager().list_procedures(graph_id=graph_id)
    else:
        raise RuntimeError("Invalid request")


def update_procedure(
    graph_id, procedure_id, update_procedure_request=None
):  # noqa: E501
    """update_procedure

    Update procedure on a graph by id # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param procedure_id:
    :type procedure_id: str
    :param update_procedure_request:
    :type update_procedure_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        update_procedure_request = UpdateProcedureRequest.from_dict(
            connexion.request.get_json()
        )
        return get_procedure_manager().update_procedure(
            graph_id=graph_id,
            procedure_id=procedure_id,
            update_procedure_request=update_procedure_request,
        )
    else:
        raise RuntimeError("Invalid request")

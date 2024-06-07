import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gscoordinator.flex.models.error import Error  # noqa: E501
from gscoordinator.flex.models.service_status import ServiceStatus  # noqa: E501
from gscoordinator.flex.models.start_service_request import StartServiceRequest  # noqa: E501
from gscoordinator.flex import util

from gscoordinator.flex.core import client_wrapper
from gscoordinator.flex.core import handle_api_exception


def get_service_status_by_id(graph_id):  # noqa: E501
    """get_service_status_by_id

    Get service status by graph ID # noqa: E501

    :param graph_id:
    :type graph_id: str

    :rtype: Union[ServiceStatus, Tuple[ServiceStatus, int], Tuple[ServiceStatus, int, Dict[str, str]]
    """
    return client_wrapper.get_service_status_by_id(graph_id)


@handle_api_exception()
def list_service_status():  # noqa: E501
    """list_service_status

    List all service status # noqa: E501


    :rtype: Union[List[ServiceStatus], Tuple[List[ServiceStatus], int], Tuple[List[ServiceStatus], int, Dict[str, str]]
    """
    return client_wrapper.list_service_status()


@handle_api_exception()
def restart_service():  # noqa: E501
    """restart_service

    Restart current service # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.restart_service()


@handle_api_exception()
def start_service(start_service_request=None):  # noqa: E501
    """start_service

    Start service # noqa: E501

    :param start_service_request: 
    :type start_service_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        start_service_request = StartServiceRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.start_service(start_service_request)


@handle_api_exception()
def stop_service():  # noqa: E501
    """stop_service

    Stop current service # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.stop_service()

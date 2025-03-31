import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_interactive_admin.models.api_response_with_code import APIResponseWithCode  # noqa: E501
from gs_interactive_admin.models.service_status import ServiceStatus  # noqa: E501
from gs_interactive_admin.models.start_service_request import StartServiceRequest  # noqa: E501
from gs_interactive_admin.models.stop_service_request import StopServiceRequest  # noqa: E501
from gs_interactive_admin import util


def check_service_ready():  # noqa: E501
    """check_service_ready

    Check if the service is ready # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return 'do some magic!'


def get_service_status():  # noqa: E501
    """get_service_status

    Get service status # noqa: E501


    :rtype: Union[ServiceStatus, Tuple[ServiceStatus, int], Tuple[ServiceStatus, int, Dict[str, str]]
    """
    return 'do some magic!'


def restart_service():  # noqa: E501
    """restart_service

    Start current service # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return 'do some magic!'


def start_service(start_service_request=None):  # noqa: E501
    """start_service

    Start service on a specified graph # noqa: E501

    :param start_service_request: Start service on a specified graph
    :type start_service_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        start_service_request = StartServiceRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def stop_service(stop_service_request=None):  # noqa: E501
    """stop_service

    Stop current service # noqa: E501

    :param stop_service_request: Stop service on a specified graph
    :type stop_service_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        stop_service_request = StopServiceRequest.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'

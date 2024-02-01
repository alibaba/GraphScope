from typing import Dict, Tuple, Union

import connexion

from gs_flex_coordinator import util
from gs_flex_coordinator.core import client_wrapper, handle_api_exception
from gs_flex_coordinator.models.service_status import \
    ServiceStatus  # noqa: E501
from gs_flex_coordinator.models.start_service_request import \
    StartServiceRequest  # noqa: E501


@handle_api_exception()
def get_service_status():  # noqa: E501
    """get_service_status

    Get service status # noqa: E501


    :rtype: Union[ServiceStatus, Tuple[ServiceStatus, int], Tuple[ServiceStatus, int, Dict[str, str]]
    """
    return client_wrapper.get_service_status()


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
        start_service_request = StartServiceRequest.from_dict(
            connexion.request.get_json()
        )  # noqa: E501
    return client_wrapper.start_service(start_service_request)


@handle_api_exception()
def stop_service():  # noqa: E501
    """stop_service

    Stop current service # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.stop_service()

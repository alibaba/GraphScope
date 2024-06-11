import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gscoordinator.flex.models.error import Error  # noqa: E501
from gscoordinator.flex.models.running_deployment_info import RunningDeploymentInfo  # noqa: E501
from gscoordinator.flex.models.running_deployment_status import RunningDeploymentStatus  # noqa: E501
from gscoordinator.flex import util

from gscoordinator.flex.core import client_wrapper
from gscoordinator.flex.core import handle_api_exception


@handle_api_exception()
def get_deployment_info():  # noqa: E501
    """get_deployment_info

    Deployment information # noqa: E501


    :rtype: Union[RunningDeploymentInfo, Tuple[RunningDeploymentInfo, int], Tuple[RunningDeploymentInfo, int, Dict[str, str]]
    """
    return client_wrapper.get_deployment_info()


def get_deployment_status():  # noqa: E501
    """get_deployment_status

    Deployment status # noqa: E501


    :rtype: Union[RunningDeploymentStatus, Tuple[RunningDeploymentStatus, int], Tuple[RunningDeploymentStatus, int, Dict[str, str]]
    """
    return client_wrapper.get_deployment_status()

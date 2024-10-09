import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gscoordinator.flex.models.error import Error  # noqa: E501
from gscoordinator.flex.models.get_pod_log_response import GetPodLogResponse  # noqa: E501
from gscoordinator.flex.models.get_resource_usage_response import GetResourceUsageResponse  # noqa: E501
from gscoordinator.flex.models.get_storage_usage_response import GetStorageUsageResponse  # noqa: E501
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


@handle_api_exception()
def get_deployment_pod_log(pod_name, component, from_cache):  # noqa: E501
    """get_deployment_pod_log

    [Deprecated] Get kubernetes pod&#39;s log # noqa: E501

    :param pod_name:
    :type pod_name: str
    :param component:
    :type component: str
    :param from_cache:
    :type from_cache: bool

    :rtype: Union[GetPodLogResponse, Tuple[GetPodLogResponse, int], Tuple[GetPodLogResponse, int, Dict[str, str]]
    """
    return client_wrapper.get_deployment_pod_log(pod_name, component, from_cache)


@handle_api_exception()
def get_deployment_resource_usage():  # noqa: E501
    """get_deployment_resource_usage

    [Deprecated] Get resource usage(cpu/memory) of cluster # noqa: E501


    :rtype: Union[GetResourceUsageResponse, Tuple[GetResourceUsageResponse, int], Tuple[GetResourceUsageResponse, int, Dict[str, str]]
    """
    return client_wrapper.get_deployment_resource_usage()


@handle_api_exception()
def get_deployment_status():  # noqa: E501
    """get_deployment_status

    Get deployment status of cluster # noqa: E501


    :rtype: Union[RunningDeploymentStatus, Tuple[RunningDeploymentStatus, int], Tuple[RunningDeploymentStatus, int, Dict[str, str]]
    """
    return client_wrapper.get_deployment_status()


@handle_api_exception()
def get_storage_usage():  # noqa: E501
    """get_storage_usage

    [Deprecated] Get storage usage of Groot # noqa: E501


    :rtype: Union[GetStorageUsageResponse, Tuple[GetStorageUsageResponse, int], Tuple[GetStorageUsageResponse, int, Dict[str, str]]
    """
    return client_wrapper.get_storage_usage()

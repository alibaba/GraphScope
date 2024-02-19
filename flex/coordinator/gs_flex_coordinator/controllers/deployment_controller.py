import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception
from gs_flex_coordinator.models.deployment_info import DeploymentInfo  # noqa: E501
from gs_flex_coordinator.models.node_status import NodeStatus  # noqa: E501
from gs_flex_coordinator import util


@handle_api_exception()
def fetch_log(component, pod_name, container_name, since_seconds):  # noqa: E501
    """fetch_log

     # noqa: E501

    :param component:
    :type component: str
    :param pod_name:
    :type pod_name: str
    :param container_name:
    :type container_name: str
    :param since_seconds:
    :type since_seconds: int

    :rtype: Union[List[DeploymentStatus], Tuple[List[DeploymentStatus], int], Tuple[List[DeploymentStatus], int, Dict[str, str]]
    """
    return 'do some magic!'


@handle_api_exception()
def get_deployment_info():  # noqa: E501
    """get_deployment_info

    Get deployment&#39;s meta info # noqa: E501


    :rtype: Union[List[DeploymentInfo], Tuple[List[DeploymentInfo], int], Tuple[List[DeploymentInfo], int, Dict[str, str]]
    """
    return client_wrapper.get_deployment_info()


@handle_api_exception()
def get_deployment_status():  # noqa: E501
    """get_deployment_status

    Get deployment&#39;s status (k8s only) # noqa: E501


    :rtype: Union[List[DeploymentStatus], Tuple[List[DeploymentStatus], int], Tuple[List[DeploymentStatus], int, Dict[str, str]]
    """
    return client_wrapper.get_deployment_status()


@handle_api_exception()
def get_node_status():  # noqa: E501
    """get_node_status

    Get node status (cpu/memory/disk) # noqa: E501


    :rtype: Union[List[NodeStatus], Tuple[List[NodeStatus], int], Tuple[List[NodeStatus], int, Dict[str, str]]
    """
    return client_wrapper.get_node_status()

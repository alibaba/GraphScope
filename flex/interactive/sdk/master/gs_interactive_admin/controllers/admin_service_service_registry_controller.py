import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_interactive_admin.models.graph_service_registry_record import (
    GraphServiceRegistryRecord,
)  # noqa: E501
from gs_interactive_admin import util

from gs_interactive_admin.core.service_discovery.service_registry import (
    get_service_registry,
)


def get_service_registry_info(graph_id, service_name):  # noqa: E501
    """get_service_registry_info

    Get a service registry by graph_id and service_name # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param service_name:
    :type service_name: str

    :rtype: Union[GraphServiceRegistryRecord, Tuple[GraphServiceRegistryRecord, int], Tuple[GraphServiceRegistryRecord, int, Dict[str, str]]
    """
    return get_service_registry().discover(graph_id, service_name)


def list_service_registry_info():  # noqa: E501
    """list_service_registry_info

    List all services registry # noqa: E501


    :rtype: Union[List[List[GraphServiceRegistryRecord]], Tuple[List[List[GraphServiceRegistryRecord]], int], Tuple[List[List[GraphServiceRegistryRecord]], int, Dict[str, str]]
    """
    return get_service_registry().list_all()

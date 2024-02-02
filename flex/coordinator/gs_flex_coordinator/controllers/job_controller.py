import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception
from gs_flex_coordinator.models.job_status import JobStatus  # noqa: E501
from gs_flex_coordinator.models.schema_mapping import SchemaMapping  # noqa: E501
from gs_flex_coordinator import util


@handle_api_exception()
def create_dataloading_job(graph_name, schema_mapping):  # noqa: E501
    """create_dataloading_job

     # noqa: E501

    :param graph_name: 
    :type graph_name: str
    :param schema_mapping: 
    :type schema_mapping: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        schema_mapping = SchemaMapping.from_dict(connexion.request.get_json())  # noqa: E501
    return client_wrapper.create_dataloading_job(graph_name, schema_mapping)


@handle_api_exception()
def delete_job_by_id(job_id):  # noqa: E501
    """delete_job_by_id

     # noqa: E501

    :param job_id: 
    :type job_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.delete_job_by_id(job_id)


@handle_api_exception()
def get_job_by_id(job_id):  # noqa: E501
    """get_job_by_id

     # noqa: E501

    :param job_id: 
    :type job_id: str

    :rtype: Union[JobStatus, Tuple[JobStatus, int], Tuple[JobStatus, int, Dict[str, str]]
    """
    return client_wrapper.get_job_by_id(job_id)


@handle_api_exception()
def list_jobs():  # noqa: E501
    """list_jobs

     # noqa: E501


    :rtype: Union[List[JobStatus], Tuple[List[JobStatus], int], Tuple[List[JobStatus], int, Dict[str, str]]
    """
    return client_wrapper.list_jobs()

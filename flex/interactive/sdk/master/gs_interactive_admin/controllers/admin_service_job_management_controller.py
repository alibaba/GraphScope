import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin import util


def delete_job_by_id(job_id):  # noqa: E501
    """delete_job_by_id

     # noqa: E501

    :param job_id: 
    :type job_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return 'do some magic!'


def get_job_by_id(job_id):  # noqa: E501
    """get_job_by_id

     # noqa: E501

    :param job_id: The id of the job, returned from POST /v1/graph/{graph_id}/dataloading
    :type job_id: str

    :rtype: Union[JobStatus, Tuple[JobStatus, int], Tuple[JobStatus, int, Dict[str, str]]
    """
    return 'do some magic!'


def list_jobs():  # noqa: E501
    """list_jobs

     # noqa: E501


    :rtype: Union[List[JobStatus], Tuple[List[JobStatus], int], Tuple[List[JobStatus], int, Dict[str, str]]
    """
    return 'do some magic!'

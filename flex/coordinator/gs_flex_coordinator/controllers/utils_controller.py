import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception
from gs_flex_coordinator import util


@handle_api_exception()
def upload_file(filestorage=None):  # noqa: E501
    """upload_file

     # noqa: E501

    :param filestorage:
    :type filestorage: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return client_wrapper.upload_file(filestorage)

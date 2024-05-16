import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_flex_coordinator.models.error import Error  # noqa: E501
from gs_flex_coordinator.models.upload_file_response import UploadFileResponse  # noqa: E501
from gs_flex_coordinator import util

from gs_flex_coordinator.core import client_wrapper
from gs_flex_coordinator.core import handle_api_exception


@handle_api_exception()
def upload_file(filestorage=None):  # noqa: E501
    """upload_file

     # noqa: E501

    :param filestorage: 
    :type filestorage: str

    :rtype: Union[UploadFileResponse, Tuple[UploadFileResponse, int], Tuple[UploadFileResponse, int, Dict[str, str]]
    """
    return client_wrapper.upload_file(filestorage)

import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gscoordinator.flex.models.error import Error  # noqa: E501
from gscoordinator.flex.models.upload_file_response import UploadFileResponse  # noqa: E501
from gscoordinator.flex import util

from gscoordinator.flex.core import client_wrapper
from gscoordinator.flex.core import handle_api_exception


@handle_api_exception()
def upload_file(filestorage=None):  # noqa: E501
    """upload_file

     # noqa: E501

    :param filestorage: 
    :type filestorage: str

    :rtype: Union[UploadFileResponse, Tuple[UploadFileResponse, int], Tuple[UploadFileResponse, int, Dict[str, str]]
    """
    return client_wrapper.upload_file(filestorage)

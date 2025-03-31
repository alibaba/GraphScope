import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_interactive_admin.models.api_response_with_code import APIResponseWithCode  # noqa: E501
from gs_interactive_admin.models.upload_file_response import UploadFileResponse  # noqa: E501
from gs_interactive_admin import util


def upload_file(filestorage=None):  # noqa: E501
    """upload_file

     # noqa: E501

    :param filestorage: 
    :type filestorage: str

    :rtype: Union[UploadFileResponse, Tuple[UploadFileResponse, int], Tuple[UploadFileResponse, int, Dict[str, str]]
    """
    return 'do some magic!'

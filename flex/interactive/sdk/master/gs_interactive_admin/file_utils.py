
import datetime

import typing
import subprocess
import threading
import os
from gs_interactive_admin import typing_utils
import logging
import re
from gs_interactive_admin.core.config import INTERACTIVE_WORKSPACE
from gs_interactive_admin.models.upload_file_response import UploadFileResponse

logger = logging.getLogger("interactive")

def parse_file_metadata(location: str) -> dict:
    """
    Args:
        location: optional values:
            odps://path/to/file, hdfs://path/to/file, file:///path/to/file
            /home/graphscope/path/to/file
    """
    metadata = {"datasource": "file"}
    path = location
    pattern = r"^(odps|hdfs|file|oss|s3)?://([\w/.-]+)$"
    match = re.match(pattern, location)
    if match:
        datasource = match.group(1)
        metadata["datasource"] = datasource
        if datasource == "file":
            path = match.group(2)
    if metadata["datasource"] == "file":
        _, file_extension = os.path.splitext(path)
        metadata["file_type"] = file_extension[1:]
    return metadata

def upload_file_impl(filestorage) -> str:
    filepath = os.path.join(INTERACTIVE_WORKSPACE, filestorage.filename)
    if not os.path.exists(INTERACTIVE_WORKSPACE):
        os.makedirs(INTERACTIVE_WORKSPACE)
    filestorage.save(filepath)
    metadata = parse_file_metadata(filepath)
    return UploadFileResponse.from_dict(
        {"file_path": filepath, "metadata": metadata}
    )
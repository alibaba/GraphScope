#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
from typing import Dict
from typing import Tuple
from typing import Union

import connexion

from gs_interactive_admin import util
from gs_interactive_admin.models.api_response_with_code import (  # noqa: E501
    APIResponseWithCode,
)
from gs_interactive_admin.models.service_status import ServiceStatus  # noqa: E501
from gs_interactive_admin.models.start_service_request import (  # noqa: E501
    StartServiceRequest,
)
from gs_interactive_admin.models.stop_service_request import (  # noqa: E501
    StopServiceRequest,
)

logger = logging.getLogger("interactive")


def check_service_ready():  # noqa: E501
    """check_service_ready

    Check if the service is ready # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def get_service_status():  # noqa: E501
    """get_service_status

    Get service status # noqa: E501


    :rtype: Union[ServiceStatus, Tuple[ServiceStatus, int], Tuple[ServiceStatus, int, Dict[str, str]]
    """
    return "do some magic!"


def restart_service():  # noqa: E501
    """restart_service

    Start current service # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def start_service(start_service_request=None):  # noqa: E501
    """start_service

    Start service on a specified graph # noqa: E501

    :param start_service_request: Start service on a specified graph
    :type start_service_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        start_service_request = StartServiceRequest.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"


def stop_service(stop_service_request=None):  # noqa: E501
    """stop_service

    Stop current service # noqa: E501

    :param stop_service_request: Stop service on a specified graph
    :type stop_service_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        stop_service_request = StopServiceRequest.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"

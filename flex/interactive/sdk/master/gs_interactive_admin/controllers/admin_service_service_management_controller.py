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
import connexion
from typing import Dict
from typing import Tuple
from typing import Union

from gs_interactive_admin.models.api_response_with_code import (
    APIResponseWithCode,
)  # noqa: E501
from gs_interactive_admin.models.service_status import ServiceStatus  # noqa: E501
from gs_interactive_admin.models.start_service_request import (
    StartServiceRequest,
)  # noqa: E501
from gs_interactive_admin.models.stop_service_request import (
    StopServiceRequest,
)  # noqa: E501
from gs_interactive_admin import util

from gs_interactive_admin.core.service.service_manager import get_service_manager


def get_service_status():  # noqa: E501
    """get_service_status

    Get service status # noqa: E501


    :rtype: Union[ServiceStatus, Tuple[ServiceStatus, int], Tuple[ServiceStatus, int, Dict[str, str]]
    """
    return get_service_manager().get_service_status()


def restart_service(start_service_request=None):  # noqa: E501
    """restart_service

    Start current service # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        start_service_request = StartServiceRequest.from_dict(
            connexion.request.get_json()
        )  # noqa: E501
        return get_service_manager().restart_service(start_service_request)
    else:
        raise RuntimeError("Invalid request")


def start_service(start_service_request=None):  # noqa: E501
    """start_service

    Start service on a specified graph # noqa: E501

    :param start_service_request: Start service on a specified graph
    :type start_service_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        start_service_request = StartServiceRequest.from_dict(
            connexion.request.get_json()
        )  # noqa: E501
        return get_service_manager().start_service(start_service_request)
    else:
        raise RuntimeError("Invalid request")


def stop_service(stop_service_request=None):  # noqa: E501
    """stop_service

    Stop current service # noqa: E501

    :param stop_service_request: Stop service on a specified graph
    :type stop_service_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        stop_service_request = StopServiceRequest.from_dict(
            connexion.request.get_json()
        )  # noqa: E501
        return get_service_manager().stop_service(stop_service_request)
    else:
        raise RuntimeError("Invalid request")


def check_service_ready():  # noqa: E501
    """check_service_ready

    Check if service is ready # noqa: E501


    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return get_service_manager().check_service_ready()

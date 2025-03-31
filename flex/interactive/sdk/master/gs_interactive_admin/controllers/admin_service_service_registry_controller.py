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
from gs_interactive_admin.models.graph_service_registry_record import (  # noqa: E501
    GraphServiceRegistryRecord,
)

logger = logging.getLogger("interactive")


def get_service_registry_info(graph_id, service_name):  # noqa: E501
    """get_service_registry_info

    Get a service registry by graph_id and service_name # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param service_name:
    :type service_name: str

    :rtype: Union[GraphServiceRegistryRecord, Tuple[GraphServiceRegistryRecord, int], Tuple[GraphServiceRegistryRecord, int, Dict[str, str]]
    """
    return "do some magic!"


def list_service_registry_info():  # noqa: E501
    """list_service_registry_info

    List all services registry # noqa: E501


    :rtype: Union[List[GraphServiceRegistryRecord], Tuple[List[GraphServiceRegistryRecord], int], Tuple[List[GraphServiceRegistryRecord], int, Dict[str, str]]
    """
    return "do some magic!"

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

from typing import Dict
from typing import Tuple
from typing import Union

import connexion

from gs_interactive_admin import util
from gs_interactive_admin.models.api_response_with_code import (  # noqa: E501
    APIResponseWithCode,
)


def call_proc(graph_id, body=None):  # noqa: E501
    """run queries on graph

    After the procedure is created, user can use this API to run the procedure.  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param body:
    :type body: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def call_proc_current(body=None):  # noqa: E501
    """run queries on the running graph

    Submit a query to the running graph.  # noqa: E501

    :param body:
    :type body: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def run_adhoc(graph_id, body=None):  # noqa: E501
    """Submit adhoc query to the Interactive Query Service.

    Submit a adhoc query to the running graph. The adhoc query should be represented by the physical plan: https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/physical.proto  # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param body:
    :type body: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def run_adhoc_current(body=None):  # noqa: E501
    """Submit adhoc query to the Interactive Query Service.

    Submit a adhoc query to the running graph. The adhoc query should be represented by the physical plan: https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/ir/proto/physical.proto  # noqa: E501

    :param body:
    :type body: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"

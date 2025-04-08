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
from gs_interactive_admin.models.create_procedure_request import (  # noqa: E501
    CreateProcedureRequest,
)
from gs_interactive_admin.models.create_procedure_response import (  # noqa: E501
    CreateProcedureResponse,
)
from gs_interactive_admin.models.get_procedure_response import (  # noqa: E501
    GetProcedureResponse,
)
from gs_interactive_admin.models.update_procedure_request import (  # noqa: E501
    UpdateProcedureRequest,
)

logger = logging.getLogger("interactive")


def create_procedure(graph_id, create_procedure_request):  # noqa: E501
    """create_procedure

    Create a new procedure on a graph # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param create_procedure_request:
    :type create_procedure_request: dict | bytes

    :rtype: Union[CreateProcedureResponse, Tuple[CreateProcedureResponse, int], Tuple[CreateProcedureResponse, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        create_procedure_request = CreateProcedureRequest.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"


def delete_procedure(graph_id, procedure_id):  # noqa: E501
    """delete_procedure

    Delete a procedure on a graph by id # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param procedure_id:
    :type procedure_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return "do some magic!"


def get_procedure(graph_id, procedure_id):  # noqa: E501
    """get_procedure

    Get a procedure by id # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param procedure_id:
    :type procedure_id: str

    :rtype: Union[GetProcedureResponse, Tuple[GetProcedureResponse, int], Tuple[GetProcedureResponse, int, Dict[str, str]]
    """
    return "do some magic!"


def list_procedures(graph_id):  # noqa: E501
    """list_procedures

    List all procedures # noqa: E501

    :param graph_id:
    :type graph_id: str

    :rtype: Union[List[GetProcedureResponse], Tuple[List[GetProcedureResponse], int], Tuple[List[GetProcedureResponse], int, Dict[str, str]]
    """
    return "do some magic!"


def update_procedure(
    graph_id, procedure_id, update_procedure_request=None
):  # noqa: E501
    """update_procedure

    Update procedure on a graph by id # noqa: E501

    :param graph_id:
    :type graph_id: str
    :param procedure_id:
    :type procedure_id: str
    :param update_procedure_request:
    :type update_procedure_request: dict | bytes

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    if connexion.request.is_json:
        update_procedure_request = UpdateProcedureRequest.from_dict(  # noqa: F841
            connexion.request.get_json()
        )  # noqa: E501
    return "do some magic!"

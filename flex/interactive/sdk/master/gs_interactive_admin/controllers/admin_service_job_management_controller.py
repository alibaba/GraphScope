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

from gs_interactive_admin.models.job_status import JobStatus  # noqa: E501
from gs_interactive_admin import util
from gs_interactive_admin.core.job.job_manager import get_job_manager
import logging


def delete_job_by_id(job_id):  # noqa: E501
    """delete_job_by_id

     # noqa: E501

    :param job_id:
    :type job_id: str

    :rtype: Union[str, Tuple[str, int], Tuple[str, int, Dict[str, str]]
    """
    return get_job_manager().delete_job_by_id(job_id)


def get_job_by_id(job_id):  # noqa: E501
    """get_job_by_id

     # noqa: E501

    :param job_id: The id of the job, returned from POST /v1/graph/{graph_id}/dataloading
    :type job_id: str

    :rtype: Union[JobStatus, Tuple[JobStatus, int], Tuple[JobStatus, int, Dict[str, str]]
    """
    logging.info("Get job by id: %s", job_id)
    data = get_job_manager().get_job_by_id(job_id)
    return JobStatus.from_dict(data)


def list_jobs():  # noqa: E501
    """list_jobs

     # noqa: E501


    :rtype: Union[List[JobStatus], Tuple[List[JobStatus], int], Tuple[List[JobStatus], int, Dict[str, str]]
    """
    ret_list = [JobStatus.from_dict(data) for data in get_job_manager().list_jobs()]
    logging.info("List jobs: %s", ret_list)
    return ret_list

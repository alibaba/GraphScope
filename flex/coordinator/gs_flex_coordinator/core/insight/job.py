#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2024 Alibaba Group Holding Limited.
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

from gs_flex_coordinator.core.scheduler import Scheduler
from gs_flex_coordinator.models import JobStatus


class DataloadingJobScheduler(Scheduler):
    def __init__(self, job_config, data_source, job_status):
        # {'vertices': ['person', 'software'], 'edges': [{'type_name': 'knows', 'source_vertex': 'person', 'destination_vertex': 'person'}], 'schedule': 'now', 'repeat': 'once'}
        super().__init__(job_config["schedule"], job_config["repeat"])
        self._type = "Data Import"
        self._job_config = job_config
        self._data_source = data_source
        # register the current status
        self._job_status = job_status
        # detailed information
        self._detail = self._construct_detailed_info()
        # start
        self.start()

    def _construct_detailed_info(self):
        label_list = []
        for vlabel in self._job_config["vertices"]:
            label_list.append(vlabel)
        for e in self._job_config["edges"]:
            label_list.append(
                self.get_edge_full_label(
                    e["type_name"], e["source_vertex"], e["destination_vertex"]
                )
            )
        detail = {}
        detail["label"] = ",".join(label_list)
        return detail

    def get_edge_full_label(
        self, type_name: str, source_vertex_type: str, destination_vertex_type: str
    ) -> str:
        return f"{source_vertex_type}_{type_name}_{destination_vertex_type}"

    def run(self):
        """This function needs to handle exception by itself"""
        pass

#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited.
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

from queue import Queue

from tqdm import tqdm
from tqdm.utils import ObjectWrapper

from gscoordinator.monitor import Monitor


class LoadingProgressTracker:
    progbar = None
    cur_stub = 0

    stubs = [
        "PROGRESS--GRAPH-LOADING-DESCRIPTION-",
        "PROGRESS--GRAPH-LOADING-READ-VERTEX-0",
        "PROGRESS--GRAPH-LOADING-READ-VERTEX-100",
        "PROGRESS--GRAPH-LOADING-READ-EDGE-0",
        "PROGRESS--GRAPH-LOADING-READ-EDGE-100",
        "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-0",
        "PROGRESS--GRAPH-LOADING-CONSTRUCT-VERTEX-100",
        "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-0",
        "PROGRESS--GRAPH-LOADING-CONSTRUCT-EDGE-100",
        "PROGRESS--GRAPH-LOADING-SEAL-0",
        "PROGRESS--GRAPH-LOADING-SEAL-100",
    ]


class StdStreamWrapper(ObjectWrapper):
    def __init__(self, std_stream, queue=None, drop=True):
        super().__init__(std_stream)
        self._stream_backup = std_stream
        self._lines = queue if queue is not None else Queue()
        self._drop = drop

    @property
    def stdout(self):
        return self._stream_backup

    @property
    def stderr(self):
        return self._stream_backup

    def drop(self, drop=True):
        self._drop = drop

    def write(self, line):
        line = self._filter_progress(line)
        if line is None:
            return

        label_p = Monitor.label_pat.match(line)
        if label_p is not None:
            Monitor.app_name, Monitor.graph_name = label_p.group(1), label_p.group(2)
        data_p = Monitor.data_pat.match(line)
        if data_p is not None:
            round_name, val = data_p.group(1), data_p.group(2)
            if round_name == "PEval":
                round_name = "00"
            else:
                round_name = round_name.split("-")[-1].strip()
                if len(round_name) == 1:
                    round_name = "0" + round_name
            Monitor.analyticalPerformace.add_metric(
                [Monitor.app_name, Monitor.graph_name, round_name], float(val)
            )

        self._stream_backup.write(line)
        self._stream_backup.flush()
        line = line.encode("utf-8", "ignore").decode("utf-8")
        if not self._drop:
            self._lines.put(line)

    def flush(self):
        self._stream_backup.flush()

    def poll(self, block=True, timeout=None):
        return self._lines.get(block=block, timeout=timeout)

    def _show_progress(self, line):
        total = len(LoadingProgressTracker.stubs) - 1
        if "PROGRESS--GRAPH-LOADING-DESCRIPTION-" in line:
            if LoadingProgressTracker.progbar is not None:
                LoadingProgressTracker.progbar.close()
            desc = line.split("-")[-1].strip()
            LoadingProgressTracker.progbar = tqdm(desc=desc, total=total, file=self)
        elif LoadingProgressTracker.progbar is not None:
            LoadingProgressTracker.progbar.update(1)
            LoadingProgressTracker.cur_stub += 1
            if LoadingProgressTracker.cur_stub == total:
                LoadingProgressTracker.cur_stub = 0
                LoadingProgressTracker.progbar.close()
                LoadingProgressTracker.progbar = None
                self.flush()

    def _filter_progress(self, line):
        # print('show_progress: ', len(line), ", ", line)
        if "PROGRESS--GRAPH" not in line:
            return line
        self._show_progress(line)
        return None

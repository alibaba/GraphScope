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

import sys
import threading
from queue import Queue

from tqdm import tqdm


class LoadingProgressTracker:
    progbar = None
    cur_stub = 0

    stubs = [
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


class StdStreamWrapper(object):
    def __init__(self, std_stream, queue=None, drop=True):
        self._stream_backup = std_stream
        if queue is None:
            self._lines = Queue()
        else:
            self._lines = queue
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
        line = line.encode("ascii", "ignore").decode("ascii")
        self._stream_backup.write(line)
        if not self._drop:
            self._lines.put(line)

    def flush(self):
        self._stream_backup.flush()

    def poll(self, block=True, timeout=None):
        return self._lines.get(block=block, timeout=timeout)

    def _show_progress(self):
        total = len(LoadingProgressTracker.stubs)
        if LoadingProgressTracker.progbar is None:
            LoadingProgressTracker.progbar = tqdm(
                desc="Loading Graph", total=total, file=sys.stderr
            )
        LoadingProgressTracker.progbar.update(1)
        LoadingProgressTracker.cur_stub += 1
        if LoadingProgressTracker.cur_stub == total:
            LoadingProgressTracker.cur_stub = 0
            LoadingProgressTracker.progbar.close()
            LoadingProgressTracker.progbar = None
            sys.stderr.flush()

    def _filter_progress(self, line):
        # print('show_progress: ', len(line), ", ", line)
        if "PROGRESS--GRAPH" not in line:
            return line
        self._show_progress()
        return None

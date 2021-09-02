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

import threading
from queue import Queue


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
        line = line.encode("ascii", "ignore").decode("ascii")
        self._stream_backup.write(line)
        if not self._drop:
            self._lines.put(line)

    def flush(self):
        self._stream_backup.flush()

    def poll(self, block=True, timeout=None):
        return self._lines.get(block=block, timeout=timeout)


class PipeWatcher(object):
    def __init__(self, pipe, sink, queue=None, drop=True):
        """Watch a pipe, and buffer its output if drop is False."""
        self._pipe = pipe
        self._sink = sink
        self._drop = drop
        if queue is None:
            self._lines = Queue()
        else:
            self._lines = queue

        def read_and_poll(self):
            for line in self._pipe:
                try:
                    self._sink.write(line)
                except:  # noqa: E722
                    pass
                try:
                    if not self._drop:
                        self._lines.put(line)
                except:  # noqa: E722
                    pass

        self._polling_thread = threading.Thread(target=read_and_poll, args=(self,))
        self._polling_thread.daemon = True
        self._polling_thread.start()

    def poll(self, block=True, timeout=None):
        return self._lines.get(block=block, timeout=timeout)

    def drop(self, drop=True):
        self._drop = drop

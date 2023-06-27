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

"""Monitor coordinator by prometheus"""
import copy
import functools
import re
import time
import timeit

import prometheus_client
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import start_http_server
from prometheus_client.metrics_core import GaugeMetricFamily

op_name_dict = {
    0: "CREATE_GRAPH",
    1: "BIND_APP",
    2: "CREATE_APP",
    3: "MODIFY_VERTICES",
    4: "MODIFY_EDGES",
    5: "RUN_APP",
    6: "UNLOAD_APP",
    7: "UNLOAD_GRAPH",
    8: "REPARTITION",
    9: "TRANSFORM_GRAPH",
    10: "REPORT_GRAPH",
    11: "PROJECT_GRAPH",
    12: "PROJECT_TO_SIMPLE",
    13: "COPY_GRAPH",
    14: "ADD_VERTICES",
    15: "ADD_EDGES",
    16: "ADD_LABELS",
    17: "TO_DIRECTED",
    18: "TO_UNDIRECTED",
    19: "CLEAR_EDGES",
    20: "CLEAR_GRAPH",
    21: "VIEW_GRAPH",
    22: "INDUCE_SUBGRAPH",
    23: "UNLOAD_CONTEXT",
    32: "SUBGRAPH",
    46: "DATA_SOURCE",
    47: "DATA_SINK",
    50: "CONTEXT_TO_NUMPY",
    51: "CONTEXT_TO_DATAFRAME",
    53: "TO_VINEYARD_TENSOR",
    54: "TO_VINEYARD_DATAFRAME",
    55: "ADD_COLUMN",
    56: "GRAPH_TO_NUMPY",
    57: "GRAPH_TO_DATAFRAME",
    58: "REGISTER_GRAPH_TYPE",
    59: "GET_CONTEXT_DATA",
    60: "OUTPUT",
    80: "FROM_NUMPY",
    81: "FROM_DATAFRAME",
    82: "FROM_FILE",
    90: "GET_ENGINE_CONFIG",
}

prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)


class TemGauge(object):
    """A temporary Gauge.
    It will clear the old metrics once they are collected.
    """

    def __init__(self, name: str, docs: str, labels: list) -> None:
        self.name = name
        self.docs = docs
        self.labels = labels
        self.metrics = []

    def collect(self):
        metrics = copy.copy(self.metrics)
        self.metrics = []
        return metrics

    def add_metric(self, label_names, value):
        if not isinstance(label_names, list):
            raise TypeError("label_names must be a list")
        if len(label_names) != len(self.labels):
            raise ValueError(
                "{0} labels are expected, but {1} labels are given".format(
                    len(self.labels), len(label_names)
                )
            )
        g = GaugeMetricFamily(self.name, self.docs, labels=self.labels)
        g.add_metric(label_names, value, time.time())
        self.metrics.append(g)


class Monitor:
    """This class is used to collect monitor decorators."""

    app_name = ""
    graph_name = ""

    label_pat = re.compile(r"^.+Query.+name:\s+app_(.+)_.+,.+name:\s+(.+)$")
    data_pat = re.compile(r"^.+Finished\s+(.+val.*),\s+time:\s+(.+)\s+.+$")

    sessionState = Gauge(
        "session_state",
        "The session's state: 1 stands for connected or 0 stands for closed",
    )

    analyticalRequestCounter = Counter(
        "analytical_request", "Count requests of analytical requests"
    )
    # analyticalRequestGauge = Gauge("analytical_request_time", "The analytical opration task time", ["op_name"])
    analyticalRequestGauge = TemGauge(
        "analytical_request_time", "The analytical operation task time", ["op_name"]
    )

    interactiveRequestCounter = Counter(
        "interactive_request", "Count requests of interactive requests"
    )
    interactiveRequestGauge = Gauge(
        "interactive_request_time", "The interactive operation task time", ["op_name"]
    )

    analyticalPerformance = TemGauge(
        "analytical_performance",
        "The analytical operation task time of each round",
        ["app", "graph", "round"],
    )

    prometheus_client.REGISTRY.register(analyticalPerformance)
    prometheus_client.REGISTRY.register(analyticalRequestGauge)

    @classmethod
    def startServer(cls, port=9968, addr="0.0.0.0"):
        start_http_server(port=port, addr=addr)

    @classmethod
    def connectSession(cls, func):
        @functools.wraps(func)
        def connectSessionWarp(*args, **kwargs):
            result = func(*args, **kwargs)
            if result and result.session_id:
                cls.sessionState.set(1)
            return result

        return connectSessionWarp

    @classmethod
    def closeSession(cls, func):
        @functools.wraps(func)
        def closeSessionWrap(instance, request, context):
            if request and request.session_id:
                cls.sessionState.set(0)
            return func(instance, request, context)

        return closeSessionWrap

    @classmethod
    def cleanup(cls, func):
        @functools.wraps(func)
        def cleanupWrap(instance, *args, **kwargs):
            func(instance, *args, **kwargs)
            cls.sessionState.set(0)
            return

        return cleanupWrap

    @classmethod
    def runOnAnalyticalEngine(cls, func):
        @functools.wraps(func)
        def runOnAnalyticalEngineWarp(instance, dag_def, dag_bodies, loader_op_bodies):
            cls.analyticalRequestCounter.inc()

            start_time = timeit.default_timer()
            res = func(instance, dag_def, dag_bodies, loader_op_bodies)
            end_time = timeit.default_timer()

            ops = dag_def.op
            op_name = cls.__get_op_name(ops)
            cls.analyticalRequestGauge.add_metric([op_name], end_time - start_time)
            return res

        return runOnAnalyticalEngineWarp

    @classmethod
    def runOnInteractiveEngine(cls, func):
        @functools.wraps(func)
        def runOnInteractiveEngineWarp(instance, dag_def):
            cls.interactiveRequestCounter.inc()

            start_time = timeit.default_timer()
            res = func(instance, dag_def)
            end_time = timeit.default_timer()

            ops = dag_def.op
            op_name = cls.__get_op_name(ops)
            cls.interactiveRequestGauge.labels(op_name).set(end_time - start_time)
            return res

        return runOnInteractiveEngineWarp

    @classmethod
    def __get_op_name(cls, ops):
        op_name = ""
        if len(ops) > 1:
            op_name = "multi_op"
        if len(ops) == 1:
            if ops[0].op and ops[0].op in op_name_dict:
                op_name = op_name_dict[ops[0].op]
        return op_name

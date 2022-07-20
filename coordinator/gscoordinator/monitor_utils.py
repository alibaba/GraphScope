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
from prometheus_client import start_http_server
from prometheus_client import Summary
from prometheus_client import Counter
from prometheus_client import Gauge
import prometheus_client
import functools
import timeit


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
    31: "CREATE_INTERACTIVE_QUERY",
    32: "SUBGRAPH",
    33: "GREMLIN_QUERYutil",
    34: "FETCH_GREMLIN_RESULT",
    35: "CLOSE_INTERACTIVE_QUERY",
    41: "CREATE_LEARNING_INSTANCE",
    42: "CLOSE_LEARNING_INSTANCE",
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
    90: "GET_ENGINE_CONFIG"
}

prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)


class Monitor:
    sessionState = Gauge("session_state", "The session's state: 1 contected or 0 closed")

    analyticalRequestCounter = Counter("analytical_request", "Count requests of analytical requests")
    # Guage or Summary?
    #  A same op on different graphs is very different, so we use "Gauge" to monitor the instant processing time but not using "Summary" or "Histogram".
    analyticalRequestGauge = Gauge("analytical_request_time", "The analytical opration task time", ["op_name"])

    interactiveRequestCounter = Counter("interactive_request", "Count requests of interactive requests")
    interactiveRequestGauge = Gauge("interactive_request_time", "The interactive opration task time", ["op_name"])


    # learningJobTotal = Counter("learning_task_total", "The learning engine's counter", ["session_id"])
    # learningMessageCounter = Counter("learning_engine_message_counter", "The learning engine's message counter", [
    #                                  "session_id", "op_name"])
    # learningTimeConsume = Summary("learning_task_time_consume", "The learning engine's time summary", [
    #                               "session_id","op_name"])

    @classmethod
    def startServer(cls, port=9968 ,addr="0.0.0.0"):
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
            session_id,ops = instance._session_id, dag_def.op
            if not session_id:
                session_id = ""
            
            start_time = timeit.default_timer()
            res = func(instance, dag_def, dag_bodies, loader_op_bodies)
            end_time = timeit.default_timer()
            
            if not ops:
                op_name = ""
            elif len(ops) > 1:
                op_name = "multi_op"
            else:
                if not ops[0].op or not ops[0].op in op_name_dict:
                    op_name = ""
                else:
                    op_name = op_name_dict[ops[0].op]

            cls.analyticalRequestGauge.labels(op_name).set(end_time - start_time)
            return res
        return runOnAnalyticalEngineWarp
        
    @classmethod
    def runOnInteractiveEngine(cls, func):
        @functools.wraps(func)
        def runOnInteractiveEngineWarp(instance, dag_def):
            cls.interactiveRequestCounter.inc()
            session_id,ops = instance._session_id, dag_def.op
            if not session_id:
                session_id = ""
            
            start_time = timeit.default_timer()
            res = func(instance, dag_def)
            end_time = timeit.default_timer()
            
            if not ops:
                op_name = ""
            elif len(ops) > 1:
                op_name = "multi_op"
            else:
                if not ops[0].op or not ops[0].op in op_name_dict:
                    op_name = ""
                else:
                    op_name = op_name_dict[ops[0].op]

            cls.interactiveRequestGauge.labels(op_name).set(end_time - start_time)
            return res
        return runOnInteractiveEngineWarp

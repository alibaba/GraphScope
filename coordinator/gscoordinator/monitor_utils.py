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
from tracemalloc import start
from prometheus_client import start_http_server
from prometheus_client import Enum
from prometheus_client import Summary
from prometheus_client import Counter

import functools
import timeit


op_names = {
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
    33: "GREMLIN_QUERY",
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


class Monitor:
    # startServer = start_http_server
    sessionStates = Enum("session_state", "The session's state: contected or closed", ["id"], states=["contected", "closed"])

    analyticalJobTotal = Counter("analytical_task_total", "The analytical engine's counter", [
                                   "session_id", "op_name"])
    analyticalMessageCounter = Counter("analytical_task_message_counter",
                                       "The analytical engine's message counter", ["session_id", "op_key", "op_name"])
    analyticalTimeConsume = Summary("analytical_task_time_consume", "The analytical task's time summary", [
                                    "session_id", "op_name"])

    interactiveJobTotal = Counter("interactive_task_total", "The interactive engine's counter", ["session_id"])
    interactiveMessageCounter = Counter("interactive_task_message_counter",
                                        "The interactive engine's message counter", ["session_id", "op_key", "op_name"])
    interactiveTimeConsume = Summary("interactive_task_time_consume", "The interactive task's time summary", [
                                     "session_id", "op_name"])

    learningJobTotal = Counter("learning_task_total", "The learning engine's counter", ["session_id"])
    learningMessageCounter = Counter("learning_engine_message_counter", "The learning engine's message counter", [
                                     "session_id", "op_name"])
    learningTimeConsume = Summary("learning_task_time_consume", "The learning engine's time summary", [
                                  "session_id","op_name"])

    @classmethod
    def startServer(cls, addr="0.0.0.0:9968"):
        addr, port = addr.split(":")
        start_http_server(port=int(port), addr=addr)

    @classmethod
    def connectSession(cls, func):
        @functools.wraps(func)
        def connectSessionWarp(*args, **kwargs):
            result = func(*args, **kwargs)
            if result and result.session_id:
                cls.sessionStates.labels(result.session_id).state("contected")
            return result
        return connectSessionWarp

    @classmethod
    def closeSession(cls, func):
        @functools.wraps(func)
        def closeSessionWrap(instance, request, context):
            if request and request.session_id:
                cls.sessionStates.labels(request.session_id).state("closed")
            return func(instance, request, context)
        return closeSessionWrap


    # TODO: 有待修改
    # run_on_analytical_engine 可以一次传递多个请求
    # 目前该装饰器只处理了第一个请求的key，把第一个请求当作所有请求
    # 有待考证
    @classmethod
    def runOnAnalyticalEngine(cls, func):
        @functools.wraps(func)
        def runOnAnalyticalEngineWarp(instance, dag_def, dag_bodies, loader_op_bodies):
            session_id,_,op_name = instance._session_id, dag_def.op[0].key, dag_def.op[0].op
            if not session_id:
                session_id = ""
            # if not op_key:
            #     op_key = ""
            if op_name == None or op_name not in op_names:
                op_name = ""
            else:
                op_name = op_names[op_name]

            cls.analyticalJobTotal.labels(session_id,op_name).inc()
            start_time = timeit.default_timer()
            res = func(instance, dag_def, dag_bodies, loader_op_bodies)
            end_time = timeit.default_timer()
            cls.analyticalTimeConsume.labels(
                session_id, op_name).observe(end_time - start_time)
            return res
        return runOnAnalyticalEngineWarp

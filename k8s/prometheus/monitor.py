#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2021 Alibaba Group Holding Limited.
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
import os
import sys
import time
from threading import Thread

import prometheus_client
from prometheus_client import Summary
from prometheus_client import start_http_server

prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)

Request_summary = Summary('gie_request', 'Time spent of gie request processing ', ['success'])

# metric log file in /var/log/graphscope/<random dir>/frontend/metric.log
def check_log_file():
    scope_dir = "/var/log/graphscope"
    metric_name = "frontend/metric.log"
    metric_log = ""

    # find metric.log file
    metric_log = None
    for x in os.listdir(scope_dir):
        log_file = os.path.join(scope_dir, x, metric_name)
        if os.path.exists(log_file):
            metric_log = log_file
            break
    return metric_log

# parse latest log forever
def parse_log_file(metric_log):
    with open(metric_log, "r") as f:
        while True:
            line = f.readline()
            
            
            if not line:
                time.sleep(1)
                continue
            
            list_line = line.split("|")
            if len(list_line) != 5:
                continue
            _, success, time_cost, _ = map(lambda x: x.strip(), list_line[1:])
            Request_summary.labels(success).observe(float(time_cost)/1000)

def start_parse_log():
    while True:
        metric_log = check_log_file()
        if metric_log:
            break
        else:
            time.sleep(3)
    parse_log_file(metric_log)


def start_server(port=9969, addr="0.0.0.0"):
    start_http_server(port=port, addr=addr)
    t = Thread(target=start_parse_log)
    t.start()
    t.join()




if __name__ == "__main__":
    args = sys.argv
    port = 9969
    addr = "0.0.0.0"
    if len(args) == 2:
        arg = args[1].split(":")
        addr = arg[0]
        port = int(arg[1])
    start_server(port, addr)
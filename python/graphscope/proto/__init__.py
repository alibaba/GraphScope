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

import os
import sys

# ensure graphscope.proto preponderate over outside `proto` directory.


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from . import attr_value_pb2
from . import coordinator_service_pb2
from . import coordinator_service_pb2_grpc
from . import data_types_pb2
from . import ddl_service_pb2
from . import ddl_service_pb2_grpc
from . import engine_service_pb2
from . import engine_service_pb2_grpc
from . import error_codes_pb2
from . import graph_def_pb2
from . import message_pb2
from . import op_def_pb2
from . import query_args_pb2
from . import types_pb2

del attr_value_pb2
del coordinator_service_pb2
del coordinator_service_pb2_grpc
del data_types_pb2
del ddl_service_pb2
del ddl_service_pb2_grpc
del engine_service_pb2
del engine_service_pb2_grpc
del error_codes_pb2
del graph_def_pb2
del message_pb2
del op_def_pb2
del query_args_pb2
del types_pb2

sys.path.pop(0)

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

import yaml

from graphscope.analytical.udf.compile import GRAPECompiler
from graphscope.analytical.udf.utils import InMemoryZip
from graphscope.analytical.udf.utils import LinesWrapper
from graphscope.analytical.udf.utils import ProgramModel
from graphscope.framework.app import load_app
from graphscope.framework.utils import get_timestamp


def wrap_init(
    algo, program_model, pyx_header, pyx_body, vd_type, md_type, pregel_combine
):
    """Wrapper :code:`__init__` function in algo."""
    algo_name = getattr(algo, "__name__")
    module_name = algo_name + "_" + get_timestamp(with_milliseconds=False)

    pyx_code = "\n\n".join(pyx_header.dump() + pyx_body.dump())
    gs_config = {
        "app": [
            {
                "algo": module_name,
                "context_type": "labeled_vertex_data",
                "type": (
                    "cython_pie"
                    if program_model == ProgramModel.PIE
                    else "cython_pregel"
                ),
                "class_name": "gs::PregelPropertyAppBase",
                "compatible_graph": ["vineyard::ArrowFragment"],
                "vd_type": vd_type,
                "md_type": md_type,
                "pregel_combine": pregel_combine,
            }
        ]
    }

    garfile = InMemoryZip()
    garfile.append("{}.pyx".format(module_name), pyx_code)
    garfile.append(".gs_conf.yaml", yaml.dump(gs_config))

    def init(self):
        pass

    def call(self, graph, **kwargs):
        app_assets = load_app(gar=garfile.read_bytes(), algo=module_name)
        return app_assets(graph, **kwargs)

    setattr(algo, "__decorated__", True)  # can't decorate on a decorated class
    setattr(algo, "_gar", garfile.read_bytes().getvalue())
    setattr(algo, "__init__", init)
    setattr(algo, "__call__", call)


def pyx_codegen(
    algo,
    defs,
    program_model,
    pyx_header,
    vd_type=None,
    md_type=None,
    pregel_combine=False,
):
    """Transfer python to cython code with :code:`grape.GRAPECompiler`.

    Args:
      algo: class defination of algorithm.
      defs: list of function to be transfer.
      program_model: ProgramModel, 'Pregel' or 'PIE'.
      pyx_header: LinesWrapper, list of pyx source code.
      vd_type (str): vertex data type.
      md_type (str): message type.
      pregel_combine (bool): combinator in pregel model.

    """
    class_name = getattr(algo, "__name__")

    compiler = GRAPECompiler(class_name, vd_type, md_type, program_model)

    pyx_body = LinesWrapper()
    for func_name in defs.keys():
        func = getattr(algo, func_name)
        cycode = compiler.run(func, pyx_header)
        pyx_body.putline(cycode)

    # append code body
    # pyx_wrapper['pyx_code_body'].extend(pyx_code_body)
    wrap_init(
        algo, program_model, pyx_header, pyx_body, vd_type, md_type, pregel_combine
    )

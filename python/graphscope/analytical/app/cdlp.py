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


from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for

__all__ = ["cdlp"]


@not_compatible_for("arrow_property", "dynamic_property")
def cdlp(graph, max_round=10):
    """Evaluate Community Detection with Label Propagation.

    Args:
        graph (:class:`Graph`): A projected simple graph.
        max_round (int, optional): Maximum rounds. Defaults to 10.

    Returns:
        :class:`VertexDataContext`: A context with each vertex assigned with a community ID.

    Examples:

    .. code:: python

        import graphscope as gs
        s = gs.session()
        g = s.load_from('The parameters for loading a graph...')
        pg = g.project_to_simple(v_label='vlabel', e_label='elabel')
        r = gs.cdlp(g, max_round=10)
        s.close()

    """
    max_round = int(max_round)
    return AppAssets(algo="cdlp")(graph, max_round)

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

__all__ = ["k_core"]


@not_compatible_for("arrow_property", "dynamic_property")
def k_core(graph, k: int):
    """K-cores of the graph are connected components that are left after
    all vertices of degree less than `k` have been removed.

    Args:
        graph (:class:`Graph`): A projected simple graph.
        k (int): The `k` for k-core.

    Returns:
        :class:`VertexDataContext`: A context with each vertex assigned with a boolean:
        1 if the vertex satisfies k-core, otherwise 0.

    Examples:

    .. code:: python

        import graphscope as gs
        sess = gs.session()
        g = sess.g()
        pg = g.project_to_simple(v_label='vlabel', e_label='elabel')
        r = gs.k_core(pg)
        s.close()

    """
    return AppAssets(algo="kcore")(graph, k=k)

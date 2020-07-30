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

__all__ = ["lpa"]


@not_compatible_for("dynamic_property", "arrow_projected", "dynamic_projected")
def lpa(graph, max_round=10):
    """Evaluate (multi-) label propagation on a property graph.

    Args:
        graph (:class:`Graph`): A property graph.
        max_round (int, optional): Maximum number of rounds. Defaults to 10.

    Returns:
        :class:`LabeledVertexPropertyContext`: A context with each vertex, following an array of propagated labels.

    Examples:

    .. code:: python

        import graphscope as gs
        s = gs.session()
        g = s.load_from('The parameters for loading a graph...')
        r = gs.lpa(g)
        s.close()

    """
    max_round = int(max_round)
    return AppAssets(algo="lpau2i")(graph, max_round)

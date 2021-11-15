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


import networkx as nxa
from networkx.generators.triads import TRIAD_EDGES

from graphscope.nx import DiGraph
from graphscope.nx.utils.compat import patch_docstring

__all__ = ["triad_graph"]


@patch_docstring(nxa.triad_graph)
def triad_graph(triad_name):
    """Returns the triad graph with the given name.

    Each string in the following tuple is a valid triad name::

        ('003', '012', '102', '021D', '021U', '021C', '111D', '111U',
         '030T', '030C', '201', '120D', '120U', '120C', '210', '300')

    Each triad name corresponds to one of the possible valid digraph on
    three nodes.

    Parameters
    ----------
    triad_name : string
        The name of a triad, as described above.

    Returns
    -------
    :class:`~networkx.DiGraph`
        The digraph on three nodes with the given name. The nodes of the
        graph are the single-character strings 'a', 'b', and 'c'.

    Raises
    ------
    ValueError
        If `triad_name` is not the name of a triad.

    See also
    --------
    triadic_census

    """
    if triad_name not in TRIAD_EDGES:
        raise ValueError(
            f'unknown triad name "{triad_name}"; use one of the triad names'
            " in the TRIAD_NAMES constant"
        )
    G = DiGraph()
    G.add_nodes_from("abc")
    G.add_edges_from(TRIAD_EDGES[triad_name])
    return G

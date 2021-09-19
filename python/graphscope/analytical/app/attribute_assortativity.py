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
# Author: Ning Xin
#


from graphscope.framework.app import AppAssets
from graphscope.framework.app import not_compatible_for
from graphscope.framework.app import project_to_simple

__all__ = ["attribute_assortativity_coefficient", "numeric_assortativity_coefficient"]


@project_to_simple
@not_compatible_for("arrow_property")
def attribute_assortativity_coefficient(G):
    """Compute assortativity for node attributes.

    Assortativity measures the similarity of connections
    in the graph with respect to the given attribute.

    Parameters
    ----------
    G : NetworkX graph

    Returns
    -------
    r: float
       Assortativity of graph for given attribute

    Examples
    --------
    .. code:: python

        import graphscope as gs
        sess = gs.session()
        g = sess.g()
        pg = g.project(vertices={"vlabel": []}, edges={"elabel": []})
        r = gs.attribute_assortativity_coefficient(pg)
        s.close()

    Notes
    -----
    This computes Eq. (2) in Ref. [1]_ , (trace(M)-sum(M^2))/(1-sum(M^2)),
    where M is the joint probability distribution (mixing matrix)
    of the specified attribute.

    References
    ----------
    .. [1] M. E. J. Newman, Mixing patterns in networks,
       Physical Review E, 67 026126, 2003
    """

    ctx = AppAssets(algo="attribute_assortativity_coefficient", context="tensor")(
        G, False
    )
    return ctx.to_numpy("r", axis=0)[0]


@project_to_simple
@not_compatible_for("arrow_property")
def numeric_assortativity_coefficient(G):
    """Compute assortativity for numerical node attributes.

    Assortativity measures the similarity of connections
    in the graph with respect to the given numeric attribute.

    Parameters
    ----------
    G : NetworkX graph

    attribute : string
        Node attribute key.

    Returns
    -------
    r: float
       Assortativity of graph for given attribute

    Examples
    --------
    .. code:: python

        import graphscope as gs
        sess = gs.session()
        g = sess.g()
        pg = g.project(vertices={"vlabel": []}, edges={"elabel": []})
        r = gs.numeric_assortativity_coefficient(pg)
        s.close()


    Notes
    -----
    This computes Eq. (21) in Ref. [1]_ , for the mixing matrix
    of the specified attribute.

    References
    ----------
    .. [1] M. E. J. Newman, Mixing patterns in networks
           Physical Review E, 67 026126, 2003
    """

    ctx = AppAssets(algo="attribute_assortativity_coefficient", context="tensor")(
        G, True
    )
    return ctx.to_numpy("r", axis=0)[0]

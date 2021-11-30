#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file is referred and derived from project NetworkX
#
# which has the following license:
#
# Copyright (C) 2004-2020, NetworkX Developers
# Aric Hagberg <hagberg@lanl.gov>
# Dan Schult <dschult@colgate.edu>
# Pieter Swart <swart@lanl.gov>
# All rights reserved.
#
# This file is part of NetworkX.
#
# NetworkX is distributed under a BSD license; see LICENSE.txt for more
# information.
#

import os

import pytest
from networkx.tests.test_convert_pandas import TestConvertPandas

import graphscope.nx as nx
from graphscope.nx.tests.utils import assert_edges_equal
from graphscope.nx.tests.utils import assert_graphs_equal
from graphscope.nx.tests.utils import assert_nodes_equal
from graphscope.nx.utils.compat import with_graphscope_nx_context

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestConvertPandas)
class TestConvertPandas:
    def test_edgekey_with_multigraph(self):
        pass

    def test_edgekey_with_normal_graph_no_action(self):
        pass

    def test_nonexisting_edgekey_raises(self):
        pass

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="num_worker=2: DataFrame.index values are different",
    )
    def test_from_adjacency_named(self):
        # example from issue #3105
        data = {
            "A": {"A": 0, "B": 0, "C": 0},
            "B": {"A": 1, "B": 0, "C": 0},
            "C": {"A": 0, "B": 1, "C": 0},
        }
        dftrue = pd.DataFrame(data)
        df = dftrue[["A", "C", "B"]]
        G = nx.from_pandas_adjacency(df, create_using=nx.DiGraph())
        df = nx.to_pandas_adjacency(G, dtype=np.intp)
        pd.testing.assert_frame_equal(df, dftrue)

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="num_worker=2: DataFrame.index values are different",
    )
    def test_from_adjacency(self):
        nodelist = [1, 2]
        dftrue = pd.DataFrame(
            [[1, 1], [1, 0]], dtype=int, index=nodelist, columns=nodelist
        )
        G = nx.Graph([(1, 1), (1, 2)])
        df = nx.to_pandas_adjacency(G, dtype=int)
        pd.testing.assert_frame_equal(df, dftrue)

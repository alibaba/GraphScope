"""Generators - Directed Graphs
----------------------------
"""
import pytest
from networkx.generators.tests.test_directed import TestGeneratorsDirected
from networkx.generators.tests.test_directed import TestRandomKOutGraph
from networkx.generators.tests.test_directed import TestUniformRandomKOutGraph

import graphscope.nx as nx
from graphscope.nx.classes import Graph
from graphscope.nx.classes import MultiDiGraph
from graphscope.nx.generators.directed import gn_graph
from graphscope.nx.generators.directed import gnc_graph
from graphscope.nx.generators.directed import gnr_graph
from graphscope.nx.generators.directed import random_k_out_graph
from graphscope.nx.generators.directed import random_uniform_k_out_graph
from graphscope.nx.generators.directed import scale_free_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorsDirected)
class TestGeneratorsDirected:
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestRandomKOutGraph)
class TestRandomKOutGraph:
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestUniformRandomKOutGraph)
class TestUniformRandomKOutGraph:
    pass

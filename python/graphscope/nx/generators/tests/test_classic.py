"""
====================
Generators - Classic
====================

Unit tests for various classic graph generators in generators/classic.py
"""
import pytest
from networkx.generators.tests.test_classic import TestGeneratorClassic

import graphscope.nx as nx
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorClassic)
class TestGeneratorClassic:
    @pytest.mark.skip(reason="FIXME: test take too much time.")
    def test_dorogovtsev_goltsev_mendes_graph(self):
        pass

    def test_ladder_graph(self):
        for i, G in [
            (0, nx.empty_graph(0)),
            (1, nx.path_graph(2)),
        ]:
            assert is_isomorphic(nx.ladder_graph(i), G)

        pytest.raises(nx.NetworkXError, nx.ladder_graph, 2, create_using=nx.DiGraph)

        g = nx.ladder_graph(2)
        mg = nx.ladder_graph(2, create_using=nx.MultiGraph)
        assert_edges_equal(mg.edges(), g.edges())

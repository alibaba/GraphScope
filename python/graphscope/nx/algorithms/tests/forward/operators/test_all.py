import networkx.algorithms.operators.tests.test_all
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_all,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
def test_intersection_all_attributes():
    g = nx.Graph()
    g.add_node(0, x=4)
    g.add_node(1, x=5)
    g.add_edge(0, 1, size=5)
    g.graph["name"] = "g"

    h = g.copy()
    h.graph["name"] = "h"
    h.graph["attr"] = "attr"
    h.nodes[0]["x"] = 7

    gh = nx.intersection_all([g, h])
    assert set(gh.nodes()) == set(g.nodes())
    assert set(gh.nodes()) == set(h.nodes())
    assert sorted(gh.edges()) == sorted(g.edges())
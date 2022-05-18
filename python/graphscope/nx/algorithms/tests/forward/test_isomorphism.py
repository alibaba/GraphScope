import pytest
import networkx.algorithms.isomorphism.tests.test_ismags
import networkx.algorithms.isomorphism.tests.test_isomorphism
import networkx.algorithms.isomorphism.tests.test_isomorphvf2
import networkx.algorithms.isomorphism.tests.test_temporalisomorphvf2
import networkx.algorithms.isomorphism.tests.test_match_helpers
import networkx.algorithms.isomorphism.tests.test_vf2userfunc

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_ismags,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_isomorphism,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_isomorphvf2,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_temporalisomorphvf2,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_match_helpers,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_vf2userfunc,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
def test_noncomparable_nodes():
    # NB: graphscope.nx does not support object() node, use number to replace.
    node1 = 1
    node2 = 1
    node3 = 1

    # Graph
    G = nx.path_graph([node1, node2, node3])
    gm = iso.GraphMatcher(G, G)
    assert gm.is_isomorphic()
    # Just testing some cases
    assert gm.subgraph_is_monomorphic()

    # DiGraph
    G = nx.path_graph([node1, node2, node3], create_using=nx.DiGraph)
    H = nx.path_graph([node3, node2, node1], create_using=nx.DiGraph)
    dgm = iso.DiGraphMatcher(G, H)
    assert dgm.is_isomorphic()
    # Just testing some cases
    assert gm.subgraph_is_monomorphic()


@pytest.mark.skip(reason="this test class need local test files.")
class TestVF2GraphDB(object):
    pass


@pytest.mark.skip(reason="not support multigraph")
class TestGenericMultiEdgeMatch():
    pass


@pytest.mark.skip(reason="not supoort time object as attribute")
class TestTimeRespectingGraphMatcher(object):
    pass


@pytest.mark.skip(reason="not supoort time object as attribute")
class TestDiTimeRespectingGraphMatcher(object):
    pass


@pytest.mark.usefixtures("graphscope_session")
def test_simple():
    # 16 simple tests
    w = 'weight'
    edges = [(0, 0, 1), (0, 0, 1.5), (0, 1, 2), (1, 0, 3)]
    for g1 in [
            nx.Graph(),
            nx.DiGraph(),
    ]:

        g1.add_weighted_edges_from(edges)
        g2 = g1.subgraph(g1.nodes())
        if g1.is_multigraph():
            em = iso.numerical_multiedge_match('weight', 1)
        else:
            em = iso.numerical_edge_match('weight', 1)
        assert nx.is_isomorphic(g1, g2, edge_match=em)

        for mod1, mod2 in [(False, True), (True, False), (True, True)]:
            # mod1 tests a regular edge
            # mod2 tests a selfloop
            if g2.is_multigraph():
                if mod1:
                    data1 = {0: {'weight': 10}}
                if mod2:
                    data2 = {0: {'weight': 1}, 1: {'weight': 2.5}}
            else:
                if mod1:
                    data1 = {'weight': 10}
                if mod2:
                    data2 = {'weight': 2.5}

            g2 = g1.subgraph(g1.nodes()).copy()
            if mod1:
                if not g1.is_directed():
                    g2._adj[1][0] = data1
                    g2._adj[0][1] = data1
                else:
                    g2._succ[1][0] = data1
                    g2._pred[0][1] = data1
            if mod2:
                if not g1.is_directed():
                    g2._adj[0][0] = data2
                else:
                    g2._succ[0][0] = data2
                    g2._pred[0][0] = data2

            assert not nx.is_isomorphic(g1, g2, edge_match=em)


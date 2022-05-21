import networkx.algorithms.isomorphism.tests.test_ismags
import networkx.algorithms.isomorphism.tests.test_isomorphism
import networkx.algorithms.isomorphism.tests.test_isomorphvf2
import networkx.algorithms.isomorphism.tests.test_match_helpers
import networkx.algorithms.isomorphism.tests.test_temporalisomorphvf2
import networkx.algorithms.isomorphism.tests.test_vf2userfunc
import pytest

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


@pytest.mark.skip(reason="'NeighborDict' object does not support item assignment")
def test_simple():
    pass

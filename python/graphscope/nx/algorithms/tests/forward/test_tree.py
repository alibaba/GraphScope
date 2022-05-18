import pytest
import networkx.algorithms.tree.tests.test_branchings
import networkx.algorithms.tree.tests.test_coding
import networkx.algorithms.tree.tests.test_mst
import networkx.algorithms.tree.tests.test_operations

import graphscope.nx as nx
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_branchings,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_coding,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_mst,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_operations,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestPruferSequence)
class TestPruferSequence():
    def test_inverse(self):
        for seq in product(range(4), repeat=2):
            seq2 = nx.to_prufer_sequence(nx.from_prufer_sequence(seq))
            assert list(seq) == seq2


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestNestedTuple)
class TestNestedTuple():
    @pytest.mark.skip(reason="FIXME(weibin): from_nested_tuple return a networkx graph.")
    def test_decoding(self):
        balanced = (((), ()), ((), ()))
        expected = nx.full_rary_tree(2, 2 ** 3 - 1)
        actual = nx.from_nested_tuple(balanced)
        assert nx.is_isomorphic(expected, actual)

    @pytest.mark.skip(reason="FIXME(weibin): from_nested_tuple return a networkx graph.")
    def test_sensible_relabeling(self):
        balanced = (((), ()), ((), ()))
        T = nx.from_nested_tuple(balanced, sensible_relabeling=True)
        edges = [(0, 1), (0, 2), (1, 3), (1, 4), (2, 5), (2, 6)]
        assert nodes_equal(list(T), list(range(2 ** 3 - 1)))
        assert edges_equal(list(T.edges()), edges)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(MinimumSpanningTreeTestBase)
class TestBoruvka:
    algorithm = "boruvka"

    @pytest.mark.skip(reason="orjson not support nan")
    def test_unicode_name(self):
        pass

    @pytest.mark.skip(reason="orjson not support nan")
    def test_nan_weights(self):
        pass

    @pytest.mark.skip(reason="orjson not support nan")
    def test_nan_weights_order(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
class TestTreeRecognition(object):

    graph = nx.Graph

    @classmethod
    def setup_class(cls):

        cls.T1 = cls.graph()

        cls.T2 = cls.graph()
        cls.T2.add_node(1)

        cls.T3 = cls.graph()
        cls.T3.add_nodes_from(range(5))
        edges = [(i, i + 1) for i in range(4)]
        cls.T3.add_edges_from(edges)

        cls.T6 = cls.graph()
        cls.T6.add_nodes_from([6, 7])
        cls.T6.add_edge(6, 7)

        cls.F1 = nx.compose(cls.T6, cls.T3)

        cls.N4 = cls.graph()
        cls.N4.add_node(1)
        cls.N4.add_edge(1, 1)

        cls.N5 = cls.graph()
        cls.N5.add_nodes_from(range(5))

        cls.N6 = cls.graph()
        cls.N6.add_nodes_from(range(3))
        cls.N6.add_edges_from([(0, 1), (1, 2), (2, 0)])

        cls.NF1 = nx.compose(cls.T6, cls.N6)

    def test_null_tree(self):
        with pytest.raises(nx.NetworkXPointlessConcept):
            nx.is_tree(self.graph())

    def test_null_forest(self):
        with pytest.raises(nx.NetworkXPointlessConcept):
            nx.is_forest(self.graph())

    def test_is_tree(self):
        assert nx.is_tree(self.T2)
        assert nx.is_tree(self.T3)

    def test_is_not_tree(self):
        assert not nx.is_tree(self.N4)
        assert not nx.is_tree(self.N5)
        assert not nx.is_tree(self.N6)

    def test_is_forest(self):
        assert nx.is_forest(self.T2)
        assert nx.is_forest(self.T3)
        assert nx.is_forest(self.F1)
        assert nx.is_forest(self.N5)

    def test_is_not_forest(self):
        assert not nx.is_forest(self.N4)
        assert not nx.is_forest(self.N6)
        assert not nx.is_forest(self.NF1)


@pytest.mark.usefixtures("graphscope_session")
class TestDirectedTreeRecognition(TestTreeRecognition):
    graph = nx.DiGraph


@pytest.mark.usefixtures("graphscope_session")
def test_disconnected_graph():
    # https://github.com/networkx/networkx/issues/1144
    G = nx.Graph()
    G.add_edges_from([(0, 1), (1, 2), (2, 0), (3, 4)])
    assert not nx.is_tree(G)

    G = nx.DiGraph()
    G.add_edges_from([(0, 1), (1, 2), (2, 0), (3, 4)])
    assert not nx.is_tree(G)


@pytest.mark.usefixtures("graphscope_session")
def test_dag_nontree():
    G = nx.DiGraph()
    G.add_edges_from([(0, 1), (0, 2), (1, 2)])
    assert not nx.is_tree(G)
    assert nx.is_directed_acyclic_graph(G)


@pytest.mark.usefixtures("graphscope_session")
def test_emptybranch():
    G = nx.DiGraph()
    G.add_nodes_from(range(10))
    assert nx.is_branching(G)
    assert not nx.is_arborescence(G)


@pytest.mark.usefixtures("graphscope_session")
def test_path():
    G = nx.DiGraph()
    nx.add_path(G, range(5))
    assert nx.is_branching(G)
    assert nx.is_arborescence(G)

import networkx.algorithms.tree.tests.test_branchings
import networkx.algorithms.tree.tests.test_coding
import networkx.algorithms.tree.tests.test_mst
import networkx.algorithms.tree.tests.test_operations
import networkx.algorithms.tree.tests.test_recognition
import pytest

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

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_recognition,
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
@with_graphscope_nx_context(TestKruskal)
class TestKruskal:
    @pytest.mark.skip(reason="orjson not support nan")
    def test_nan_weights(self):
        pass

    @pytest.mark.skip(reason="orjson not support nan")
    def test_nan_weights_order(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestPrim)
class TestPrim:
    @pytest.mark.skip(reason="orjson not support nan")
    def test_nan_weights(self):
        pass

    @pytest.mark.skip(reason="orjson not support nan")
    def test_nan_weights_order(self):
        pass


@pytest.mark.skip(reason="Need to check the correctness of graphscope.nx")
@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestSpanningTreeIterator)
class TestSpanningTreeIterator:
    pass


@pytest.mark.skip(reason="Need to check the correctness of graphscope.nx")
@pytest.mark.usefixtures("graphscope_session")
def test_partition_spanning_arborescence():
    pass

@pytest.mark.skip(reason="Need to check the correctness of graphscope.nx")
@pytest.mark.usefixtures("graphscope_session")
def test_arborescence_iterator_min():
    pass


@pytest.mark.skip(reason="Need to check the correctness of graphscope.nx")
@pytest.mark.usefixtures("graphscope_session")
def test_arborescence_iterator_max():
    pass

@pytest.mark.skip(reason="Need to check the correctness of graphscope.nx")
@pytest.mark.usefixtures("graphscope_session")
def test_arborescence_iterator_initial_partition():
    pass

@pytest.mark.skip(reason="No need to check multigraph of graphscope.nx")
@pytest.mark.usefixtures("graphscope_session")
class TestPrim(MultigraphMSTTestBase):
    pass

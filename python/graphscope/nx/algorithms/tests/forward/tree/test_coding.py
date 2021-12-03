import networkx.algorithms.tree.tests.test_coding
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_coding,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tree.tests.test_coding import TestNestedTuple
from networkx.algorithms.tree.tests.test_coding import TestPruferSequence


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
        assert_nodes_equal(list(T), list(range(2 ** 3 - 1)))
        assert_edges_equal(list(T.edges()), edges)

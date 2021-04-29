import networkx.algorithms.tree.tests.test_coding
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_coding,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tree.tests.test_coding import TestPruferSequence


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestPruferSequence)
class TestPruferSequence():
    def test_inverse(self):
        for seq in product(range(4), repeat=2):
            seq2 = nx.to_prufer_sequence(nx.from_prufer_sequence(seq))
            assert list(seq) == seq2

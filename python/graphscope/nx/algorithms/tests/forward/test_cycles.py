import networkx.algorithms.tests.test_cycles
import pytest
from networkx.algorithms.tests.test_cycles import TestMinimumCycles

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_cycles,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


from graphscope.nx.algorithms import minimum_cycle_basis


def assert_basis_equal(a, b):
    assert sorted(a) == sorted(b)

@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMinimumCycles)
class TestMinimumCycles:
    def test_weighted_diamond(self):
        mcb = minimum_cycle_basis(self.diamond_graph, weight="weight")
        # in graphscope.nx, answer is [[1, 2, 3, 4], [2, 3, 4]] and it's correct too.
        assert_basis_equal([sorted(c) for c in mcb], [[1, 2, 3, 4], [2, 3, 4]])

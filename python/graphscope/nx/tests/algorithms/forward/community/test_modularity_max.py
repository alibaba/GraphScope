import networkx.algorithms.community.tests.test_modularity_max
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.community.tests.test_modularity_max,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.community.tests.test_modularity_max import TestNaive


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestNaive)
class TestNaive():
    @pytest.mark.skip(reason="stuck, too long.")
    def test_karate_club(self):
        john_a = frozenset(
            [8, 14, 15, 18, 20, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33])
        mr_hi = frozenset([0, 4, 5, 6, 10, 11, 16, 19])
        overlap = frozenset([1, 2, 3, 7, 9, 12, 13, 17, 21])
        self._check_communities({john_a, overlap, mr_hi})

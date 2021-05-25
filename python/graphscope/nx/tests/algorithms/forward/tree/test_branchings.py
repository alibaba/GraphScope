import networkx.algorithms.tree.tests.test_branchings
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_branchings,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not support multigraph")
def test_edge_attribute_preservation_multigraph(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_greedy_max1(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_greedy_max2(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_greedy_max3(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_greedy_min(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_edmonds1_maxbranch(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_edmonds1_maxarbor(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_edmonds2_maxbranch(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_edmonds2_maxarbor(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_edmonds2_minarbor(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_edmonds3_minbranch1(self):
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_edmonds3_minbranch2(self):
    pass

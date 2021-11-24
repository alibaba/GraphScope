import networkx.algorithms.operators.tests.test_binary
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_binary,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not support multigraph")
def test_intersection_multigraph_attributes():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_difference_multigraph_attributes():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_symmetric_difference_multigraph():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_union_multigraph():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_disjoint_union_multigraph():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_compose_multigraph():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_full_join_multigraph():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_mixed_type_union():
    pass

import networkx.algorithms.operators.tests.test_all
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_all,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not support multigraph")
def test_intersection_all_multigraph_attributes():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_union_all_multigraph():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_mixed_type_union():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_mixed_type_disjoint_union():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_mixed_type_intersection():
    pass


@pytest.mark.skip(reason="not support multigraph")
def test_mixed_type_compose():
    pass

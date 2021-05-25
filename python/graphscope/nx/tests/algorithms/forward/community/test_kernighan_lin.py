import networkx.algorithms.community.tests.test_kernighan_lin
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.community.tests.test_kernighan_lin,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not support multigraph")
def test_multigraph():
    pass

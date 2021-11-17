import networkx.generators.tests.test_community
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.generators.tests.test_community,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)


def test_generator():
    pass


@pytest.mark.skip(reason="G.number_of_edge() not correct when number_workers=2")
def test_windmill_graph():
    pass

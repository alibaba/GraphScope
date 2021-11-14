import networkx.generators.tests.test_intersection
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.generators.tests.test_intersection,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)

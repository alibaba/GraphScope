import networkx.generators.tests.test_community
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.generators.tests.test_community,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)

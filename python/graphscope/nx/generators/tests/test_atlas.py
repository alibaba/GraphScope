import networkx.generators.tests.test_atlas
import pytest


from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.generators.tests.test_atlas,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)

import pytest
from networkx.generators.tests.test_internet_as_graphs import TestInternetASTopology

import graphscope.nx as nx
from graphscope.nx import is_directed
from graphscope.nx import neighbors
from graphscope.nx.generators.internet_as_graphs import random_internet_as_graph
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestInternetASTopology)
class TestInternetASTopology:
    pass

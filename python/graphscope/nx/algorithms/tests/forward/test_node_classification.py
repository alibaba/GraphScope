import networkx.algorithms.tests.test_node_classification
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.algorithms.tests.test_node_classification,
    decorators=pytest.mark.usefixtures("graphscope_session"))

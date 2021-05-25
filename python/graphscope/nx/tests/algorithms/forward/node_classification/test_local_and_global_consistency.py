import networkx.algorithms.node_classification.tests.test_local_and_global_consistency
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.algorithms.node_classification.tests.test_local_and_global_consistency,
    decorators=pytest.mark.usefixtures("graphscope_session"))

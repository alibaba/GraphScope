import networkx.algorithms.node_classification.tests.test_harmonic_function
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.algorithms.node_classification.tests.test_harmonic_function,
    decorators=pytest.mark.usefixtures("graphscope_session"))

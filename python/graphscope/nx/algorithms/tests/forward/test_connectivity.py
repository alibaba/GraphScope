import networkx.algorithms.connectivity.tests.test_connectivity
import networkx.algorithms.connectivity.tests.test_cuts
import networkx.algorithms.connectivity.tests.test_disjoint_paths
import networkx.algorithms.connectivity.tests.test_edge_kcomponents
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(
    networkx.algorithms.connectivity.tests.test_connectivity,
    decorators=pytest.mark.usefixtures("graphscope_session")
)
import_as_graphscope_nx(
    networkx.algorithms.connectivity.tests.test_cuts,
    decorators=pytest.mark.usefixtures("graphscope_session")
)
import_as_graphscope_nx(
    networkx.algorithms.connectivity.tests.test_disjoint_paths,
    decorators=pytest.mark.usefixtures("graphscope_session")
)

import_as_graphscope_nx(
    networkx.algorithms.connectivity.tests.test_edge_kcomponents,
    decorators=pytest.mark.usefixtures("graphscope_session")
)

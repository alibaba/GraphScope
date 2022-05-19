import networkx.algorithms.connectivity.tests.test_connectivity
import networkx.algorithms.connectivity.tests.test_cuts
import networkx.algorithms.connectivity.tests.test_disjoint_paths
import networkx.algorithms.connectivity.tests.test_edge_augmentation
import networkx.algorithms.connectivity.tests.test_edge_kcomponents
import networkx.algorithms.connectivity.tests.test_kcomponents
import networkx.algorithms.connectivity.tests.test_kcutsets
import networkx.algorithms.connectivity.tests.test_stoer_wagner
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
    networkx.algorithms.connectivity.tests.test_edge_augmentation,
    decorators=pytest.mark.usefixtures("graphscope_session")
)
import_as_graphscope_nx(
    networkx.algorithms.connectivity.tests.test_edge_kcomponents,
    decorators=pytest.mark.usefixtures("graphscope_session")
)
import_as_graphscope_nx(
    networkx.algorithms.connectivity.tests.test_kcomponents,
    decorators=pytest.mark.usefixtures("graphscope_session")
)
import_as_graphscope_nx(
    networkx.algorithms.connectivity.tests.test_kcutsets,
    decorators=pytest.mark.usefixtures("graphscope_session")
)
import_as_graphscope_nx(
    networkx.algorithms.connectivity.tests.test_stoer_wagner,
    decorators=pytest.mark.usefixtures("graphscope_session")
)

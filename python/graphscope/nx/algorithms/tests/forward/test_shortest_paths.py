import networkx.algorithms.shortest_paths.tests.test_dense
import networkx.algorithms.shortest_paths.tests.test_dense_numpy
import networkx.algorithms.shortest_paths.tests.test_generic
import networkx.algorithms.shortest_paths.tests.test_unweighted
import networkx.algorithms.shortest_paths.tests.test_weighted
import pytest
from networkx.algorithms.shortest_paths.tests.test_astar import TestAStar as _TestAStar

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(_TestAStar)
class TestAStar():
    @pytest.mark.skip(reason="not support class object as node")
    def test_unorderable_nodes():
        pass

import_as_graphscope_nx(
    networkx.algorithms.shortest_paths.tests.test_dense,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.shortest_paths.tests.test_dense_numpy,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.shortest_paths.tests.test_generic,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.shortest_paths.tests.test_unweighted,
    decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(
    networkx.algorithms.shortest_paths.tests.test_weighted,
    decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAverageShortestPathLength)
class TestAverageShortestPathLength():
    @pytest.mark.skip(reason="builtin app would not raise Error during compute")
    def test_disconnected():
        pass

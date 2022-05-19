import networkx.algorithms.traversal.tests.test_beamsearch
import networkx.algorithms.traversal.tests.test_bfs
import networkx.algorithms.traversal.tests.test_dfs
import networkx.algorithms.traversal.tests.test_edgebfs
import networkx.algorithms.traversal.tests.test_edgedfs
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.traversal.tests.test_beamsearch,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.traversal.tests.test_bfs,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.traversal.tests.test_dfs,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.traversal.tests.test_edgebfs,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.traversal.tests.test_edgedfs,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

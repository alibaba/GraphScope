import networkx.algorithms.assortativity.tests.base_test
import networkx.algorithms.assortativity.tests.test_connectivity
import networkx.algorithms.assortativity.tests.test_correlation
import networkx.algorithms.assortativity.tests.test_mixing
import networkx.algorithms.assortativity.tests.test_neighbor_degree
import networkx.algorithms.assortativity.tests.test_pairs
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

# N.B import base_test at begin
import_as_graphscope_nx(networkx.algorithms.assortativity.tests.base_test,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_connectivity,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_neighbor_degree,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_correlation,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_mixing,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_pairs,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

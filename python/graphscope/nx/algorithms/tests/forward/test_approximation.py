import networkx.algorithms.approximation.tests.test_approx_clust_coeff
import networkx.algorithms.approximation.tests.test_clique
import networkx.algorithms.approximation.tests.test_connectivity
import networkx.algorithms.approximation.tests.test_distance_measures
import networkx.algorithms.approximation.tests.test_dominating_set
import networkx.algorithms.approximation.tests.test_kcomponents
import networkx.algorithms.approximation.tests.test_matching
import networkx.algorithms.approximation.tests.test_maxcut
import networkx.algorithms.approximation.tests.test_ramsey
import networkx.algorithms.approximation.tests.test_steinertree
import networkx.algorithms.approximation.tests.test_traveling_salesman
import networkx.algorithms.approximation.tests.test_treewidth
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_approx_clust_coeff,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_clique,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_connectivity,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_distance_measures,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_dominating_set,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_kcomponents,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_matching,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_maxcut,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_ramsey,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_steinertree,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_traveling_salesman,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_treewidth,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


pytest.mark.usefixtures("graphscope_session")
pytest.mark.skip(reason="Too slow")
def test_example_1():
    pass


pytest.mark.usefixtures("graphscope_session")
pytest.mark.skip(reason="Too slow")
def test_example_1_detail_3_and_4():
    pass


pytest.mark.usefixtures("graphscope_session")
pytest.mark.skip(reason="Too slow")
def test_torrents_and_ferraro_graph():
    pass
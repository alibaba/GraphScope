import networkx.algorithms.community.tests.test_asyn_fluid
import networkx.algorithms.community.tests.test_centrality
import networkx.algorithms.community.tests.test_kclique
import networkx.algorithms.community.tests.test_kernighan_lin
import networkx.algorithms.community.tests.test_label_propagation
import networkx.algorithms.community.tests.test_louvain
import networkx.algorithms.community.tests.test_lukes
import networkx.algorithms.community.tests.test_modularity_max
import networkx.algorithms.community.tests.test_quality
import networkx.algorithms.community.tests.test_utils
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.community.tests.test_asyn_fluid,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_centrality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_kclique,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_kernighan_lin,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_label_propagation,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_louvain,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_lukes,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_modularity_max,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_quality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.community.tests.test_utils,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

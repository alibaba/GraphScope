import networkx.algorithms.link_analysis.tests.test_pagerank
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.link_analysis.tests.test_pagerank,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

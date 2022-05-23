import networkx.algorithms.tests.test_similarity
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_similarity,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

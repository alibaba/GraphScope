import networkx.algorithms.isomorphism.tests.test_isomorphism
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_isomorphism,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

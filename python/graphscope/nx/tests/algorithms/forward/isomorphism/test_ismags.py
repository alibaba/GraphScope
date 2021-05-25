import networkx.algorithms.isomorphism.tests.test_ismags
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_ismags,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

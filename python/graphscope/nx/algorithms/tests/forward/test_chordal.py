import networkx.algorithms.tests.test_chordal
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_chordal,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

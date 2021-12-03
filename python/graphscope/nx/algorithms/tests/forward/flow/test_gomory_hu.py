import networkx.algorithms.flow.tests.test_gomory_hu
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.flow.tests.test_gomory_hu,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


from networkx.algorithms.flow.tests.test_gomory_hu import TestGomoryHuTree


@pytest.mark.usefixtures("graphscope_session")
@pytest.mark.slow
@with_graphscope_nx_context(TestGomoryHuTree)
class TestGomoryHuTree:
    pass

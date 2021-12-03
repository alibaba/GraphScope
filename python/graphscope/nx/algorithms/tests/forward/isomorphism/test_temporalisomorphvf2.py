import networkx.algorithms.isomorphism.tests.test_temporalisomorphvf2
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_temporalisomorphvf2,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not supoort time object as attribute")
class TestTimeRespectingGraphMatcher(object):
    pass


@pytest.mark.skip(reason="not supoort time object as attribute")
class TestDiTimeRespectingGraphMatcher(object):
    pass

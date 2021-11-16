import pytest
from networkx.generators.tests.test_trees import test_random_tree

try:
    from graphscope.nx.generators.trees import NIL
except ImportError:
    # NetworkX >=2.6 not contains NIL
    pass

from graphscope.nx.utils import arbitrary_element
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(test_random_tree)
def test_random_tree():
    pass

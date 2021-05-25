import os

import pytest

import graphscope


@pytest.fixture(scope="session")
def graphscope_session():
    graphscope.set_option(show_log=True)
    graphscope.set_option(initializing_interactive_engine=False)

    sess = graphscope.session(cluster_type="hosts", num_workers=1)

    sess.as_default()
    yield sess
    sess.close()

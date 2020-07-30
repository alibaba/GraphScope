import os

import pytest

import graphscope


@pytest.fixture(scope="session")
def graphscope_session():
    sess = graphscope.session(run_on_local=True, show_log=True, num_workers=1)
    sess.as_default()
    yield sess
    sess.close()

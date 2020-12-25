import os

import pytest

import graphscope

graphscope.set_option(show_log=True)


@pytest.fixture(scope="session")
def graphscope_session():
    sess = graphscope.session(run_on_local=True, num_workers=1)
    sess.as_default()
    yield sess
    sess.close()

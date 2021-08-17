# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys

import pytest

import graphscope
import time
from graphscope.framework.loader import Loader
from graphscope.dataset.modern_graph import load_modern_graph

graphscope.set_option(show_log=True)
graphscope.set_option(initializing_interactive_engine=False)

@pytest.fixture(scope="function")
def sess():
    s = graphscope.session(cluster_type="hosts", num_workers=2, enable_gaia=True)
    yield s
    s.close()

def test_load_modern_graph(sess):
    graph = load_modern_graph(sess, '/home/GraphScope/interactive_engine/tests/src/main/resources/modern_graph')
    # Interactive engine
    interactive = sess.gremlin(graph)
    time.sleep(100000)

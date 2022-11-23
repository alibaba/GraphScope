#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
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
#

import os
import platform
import sys

try:
    sys.path.insert(0, os.path.dirname(__file__))

    import vineyard

    # suppress the warnings of tensorflow
    with vineyard.envvars({"TF_CPP_MIN_LOG_LEVEL": "3", "GRPC_VERBOSITY": "NONE"}):
        try:
            import tensorflow as tf
        except ImportError:
            tf = None

        if tf is not None:
            try:
                tf.get_logger().setLevel("ERROR")
            except:  # noqa: E722, pylint: disable=bare-except
                pass
            try:
                tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
            except:  # noqa: E722, pylint: disable=bare-except
                pass

            try:
                # https://www.tensorflow.org/guide/migrate
                import tensorflow.compat.v1 as tf

                tf.disable_v2_behavior()
            except ImportError:
                pass

    def reset_default_tf_graph():
        """A method to reset the tf graph to make sure we can train twice
        (or even more times) inside a single program, e.g., a jupyter notebook.
        """
        if tf is not None:
            try:
                tf.reset_default_graph()
            except:  # noqa: E722, pylint: disable=bare-except
                pass

    ctx = {"GRPC_VERBOSITY": "NONE"}
    if platform.system() != "Darwin":
        ctx["VINEYARD_USE_LOCAL_REGISTRY"] = "TRUE"
    with vineyard.envvars(ctx):
        import graphlearn
        from graphlearn.python.utils import Mask

    try:
        import examples
    except ImportError:
        pass

    from graphscope.learning.graph import Graph

except ImportError:
    pass
finally:
    sys.path.pop(sys.path.index(os.path.dirname(__file__)))

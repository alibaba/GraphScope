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


def _force_preload_libgomp():
    """Load libgomp before importing tensorflow. See also:
    - https://github.com/opencv/opencv/issues/14884
    - https://github.com/pytorch/pytorch/issues/2575
    - https://github.com/dmlc/xgboost/issues/7110#issuecomment-880841484
    """
    if platform.system() != "Linux" or platform.processor() != "aarch64":
        return

    import ctypes
    import glob

    pkgs_directory = os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "..", ".."
    )
    for lib_directory in [
        os.path.join(pkgs_directory, "graphscope_client.libs"),
        os.path.join(pkgs_directory, "tensorflow_cpu_aws.libs"),
    ]:
        for libfile in glob.glob(lib_directory + "/libgomp*.so*"):
            import sys

            try:
                ctypes.cdll.LoadLibrary(libfile)
            except:  # noqa: E722, pylint: disable=bare-except
                pass


_force_preload_libgomp()
del _force_preload_libgomp


try:
    sys.path.insert(0, os.path.dirname(__file__))

    # adapt to latest graph-learn where "import ego_data_loader" is directly used
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "examples", "tf"))

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
            except:  # noqa
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
        raise

    from graphscope.learning.graph import Graph

except ImportError:
    raise
finally:
    sys.path.pop(sys.path.index(os.path.dirname(__file__)))
    sys.path.pop(
        sys.path.index(os.path.join(os.path.dirname(__file__), "examples", "tf"))
    )

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

import json
import os
import platform
import random
import re
import shutil
import socket
import string
import subprocess
import tempfile
import threading
import time
from queue import Queue

import numpy as np
import pandas as pd
import psutil
from google.protobuf.any_pb2 import Any

try:
    from numpy import long as numpy_long
except ImportError:
    from numpy.compat import long as numpy_long

try:
    from numpy import object as numpy_object
except ImportError:
    from numpy import object_ as numpy_object

from graphscope.client.archive import OutArchive
from graphscope.framework.errors import check_argument
from graphscope.proto import attr_value_pb2
from graphscope.proto import data_types_pb2
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2


class PipeWatcher(object):
    def __init__(self, pipe, sink, queue=None, drop=True, suppressed=False):
        """Watch a pipe, and buffer its output if drop is False."""
        self._pipe = pipe
        self._sink = sink
        self._drop = drop
        self._suppressed = suppressed
        self._filters = []

        if queue is None:
            self._lines = Queue()
        else:
            self._lines = queue

        def read_and_poll(self):
            for line in self._pipe:
                if self._filter(line) and not self._suppressed:
                    try:
                        self._sink.write(line)
                        self._sink.flush()
                        if not self._drop:
                            self._lines.put(line)
                    except:  # noqa: E722
                        pass

        self._polling_thread = threading.Thread(target=read_and_poll, args=(self,))
        self._polling_thread.daemon = True
        self._polling_thread.start()

    def poll(self, block=True, timeout=None):
        return self._lines.get(block=block, timeout=timeout)

    def drop(self, drop=True):
        self._drop = drop

    def add_filter(self, func):
        if not (func in self._filters):
            self._filters.append(func)

    def _filter(self, line):
        for func in self._filters:
            if not func(line):  # assume callable - will raise if not
                return False
        return True


class PipeMerger(object):
    def __init__(self, pipe1, pipe2):
        self._queue = Queue()
        self._stop = False

        def read_and_pool(self, tag, pipe, target: Queue):
            while True:
                try:
                    msg = (pipe.poll(), "")
                    target.put(msg if tag == "out" else msg[::-1])
                except Exception:
                    time.sleep(1)
                if self._stop:
                    break

        self._pipe1_thread = threading.Thread(
            target=read_and_pool, args=(self, "out", pipe1, self._queue)
        )
        self._pipe1_thread.daemon = True

        self._pipe2_thread = threading.Thread(
            target=read_and_pool, args=(self, "err", pipe2, self._queue)
        )
        self._pipe2_thread.daemon = True

        self._pipe1_thread.start()
        self._pipe2_thread.start()

    def poll(self, block=True, timeout=None):
        return self._queue.get(block=block, timeout=timeout)

    def stop(self):
        self._stop = True


def in_notebook():
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        if shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        return False  # Other type or standard python interpreter
    except Exception:
        return False
    return False


def is_free_port(port, host="localhost", timeout=0.2):
    """Check if a port on a given host is in use or not.

    Args:
        port (int): Port number to check availability.
        host (str): Hostname to connect to and check port availability.
        timeout (float): Timeout used for socket connection.

    Returns:
        True if port is available, False otherwise.
    """
    if host == "localhost" or host == "127.0.0.1":
        try:
            return int(port) not in [
                conn.laddr.port for conn in psutil.net_connections()
            ]
        except psutil.AccessDenied:
            # back to the socket.connect
            pass

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        sock.connect((host, int(port)))
        sock.close()
    except socket.error:
        return True
    else:
        return False


def get_free_port(host="localhost", port_range=(32768, 64999)):
    """Get a free port on a given host.

    Args:
        host (str): Hostname you want to get the free port.
        port_range (tuple): Try to get free port within this range.

    Returns:
        A free port on given host.
    """
    while True:
        port = random.randint(port_range[0], port_range[1])
        if is_free_port(port, host=host):
            return port


def find_java():
    java_exec = ""
    if "JAVA_HOME" in os.environ:
        java_exec = os.path.expandvars("$JAVA_HOME/bin/java")
    if not java_exec:
        java_exec = shutil.which("java")
    if not java_exec:
        raise RuntimeError("java command not found.")
    return java_exec


def get_java_version():
    java_exec = find_java()
    pattern = r'"(\d+\.\d+\.\d+).*"'
    version = subprocess.check_output([java_exec, "-version"], stderr=subprocess.STDOUT)
    return re.search(pattern, version.decode("utf-8")).groups()[0]


def get_platform_info():
    def _get_gcc_version():
        gcc = shutil.which("gcc")
        if gcc is None:
            return None
        return subprocess.check_output([gcc, "--version"], stderr=subprocess.STDOUT)

    platform_info = (
        f"system: {platform.system()}\n"
        f"machine: {platform.machine()}\n"
        f"platform: {platform.platform()}\n"
        f"uname: {platform.uname()}\n"
        f"kernel_ver: {platform.version()}\n"
        f"mac_ver: {platform.mac_ver()}\n"
        f"gcc_ver: {_get_gcc_version()}\n"
        f"python_ver: {platform.python_version()}\n"
    )
    return platform_info


def random_string(nlen):
    """Create random string which length is `nlen`."""
    return "".join([random.choice(string.ascii_lowercase) for _ in range(nlen)])


def get_tempdir():
    return os.path.join("/", tempfile.gettempprefix())


def get_timestamp(with_milliseconds=True):
    """Get current timestamp.

    Returns:
        Str of current time in seconds since the Epoch.

    Examples:
        >>> get_timestamp()
        '1639108065.941239'

        >>> get_timestamp(with_milliseconds=False)
        '1639108065'
    """
    t = str(time.time())
    if not with_milliseconds:
        t = t.split(".")[0]
    return t


def read_file_to_bytes(file_path):
    abs_dir = os.path.abspath(os.path.expanduser(file_path))
    if os.path.isfile(abs_dir):
        with open(abs_dir, "rb") as b:
            content = b.read()
        return content
    raise IOError("No such file: " + file_path)


def i_to_attr(i: int) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(i, int))
    return attr_value_pb2.AttrValue(i=i)


def u_to_attr(i: int) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(i, int) and i >= 0)
    return attr_value_pb2.AttrValue(u=i)


def b_to_attr(b: bool) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(b, bool))
    return attr_value_pb2.AttrValue(b=b)


def s_to_attr(s: str) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(s, str))
    return attr_value_pb2.AttrValue(s=s.encode("utf-8"))


def bytes_to_attr(s: bytes) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(s, bytes))
    return attr_value_pb2.AttrValue(s=s)


def bytes_to_large_attr(s: bytes) -> attr_value_pb2.LargeAttrValue:
    check_argument(isinstance(s, bytes))
    large_attr = attr_value_pb2.LargeAttrValue()
    chunk = attr_value_pb2.Chunk(buffer=s)
    large_attr.chunk_list.items.append(chunk)
    return large_attr


def f_to_attr(f: float) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(f, float))
    return attr_value_pb2.AttrValue(f=f)


def type_to_attr(t):
    return attr_value_pb2.AttrValue(i=t)


def graph_type_to_attr(t):
    return attr_value_pb2.AttrValue(i=t)


def modify_type_to_attr(t):
    return attr_value_pb2.AttrValue(i=t)


def report_type_to_attr(t):
    return attr_value_pb2.AttrValue(i=t)


def list_str_to_attr(list_of_str):
    attr = attr_value_pb2.AttrValue()
    attr.list.s[:] = [
        item.encode("utf-8") if isinstance(item, str) else item for item in list_of_str
    ]
    return attr


def list_i_to_attr(list_i):
    attr = attr_value_pb2.AttrValue()
    attr.list.i[:] = list_i
    return attr


def graph_type_to_cpp_class(graph_type):
    if graph_type == graph_def_pb2.IMMUTABLE_EDGECUT:
        return "grape::ImmutableEdgecutFragment"
    if graph_type == graph_def_pb2.DYNAMIC_PROPERTY:
        return "gs::DynamicFragment"
    if graph_type == graph_def_pb2.DYNAMIC_PROJECTED:
        return "gs::DynamicProjectedFragment"
    if graph_type == graph_def_pb2.ARROW_PROPERTY:
        return "vineyard::ArrowFragment"
    if graph_type == graph_def_pb2.ARROW_PROJECTED:
        return "gs::ArrowProjectedFragment"
    return "null"


def pack(v):
    param = Any()
    if isinstance(v, bool):
        param.Pack(data_types_pb2.BoolValue(value=v))
    elif isinstance(v, int):
        param.Pack(data_types_pb2.Int64Value(value=v))
    elif isinstance(v, float):
        param.Pack(data_types_pb2.DoubleValue(value=v))
    elif isinstance(v, str):
        param.Pack(data_types_pb2.StringValue(value=v))
    elif isinstance(v, bytes):
        param.Pack(data_types_pb2.BytesValue(value=v))
    else:
        raise ValueError("Wrong type of query param {}".format(type(v)))
    return param


def pack_query_params(*args, **kwargs):
    params = []
    for i in args:
        params.append(pack(i))
    for _, v in kwargs.items():
        params.append(pack(v))
    return params


def is_numpy(*args):
    """Check if the input type is numpy.ndarray."""
    for arg in args:
        if arg is not None and not isinstance(arg, np.ndarray):
            raise ValueError("The parameter %s should be a numpy ndarray" % arg)


def is_file(*args):
    """Check if the input type is file."""
    for arg in args:
        if arg is not None and not isinstance(arg, str):
            raise ValueError("the parameter %s should be a file path" % arg)


def _context_protocol_to_numpy_dtype(dtype):
    dtype_map = {
        0: np.dtype("void"),
        1: np.dtype("bool"),
        2: np.dtype("int32"),
        3: np.dtype("uint32"),
        4: np.dtype("int64"),
        5: np.dtype("uint64"),
        6: np.dtype("float32"),
        7: np.dtype("float64"),
        8: object,  # string
    }
    npdtype = dtype_map.get(dtype)
    if npdtype is None:
        raise NotImplementedError("Don't support type {}".format(dtype))
    return npdtype


def decode_numpy(value):
    if not value:
        raise RuntimeError("Value to decode should not be empty")
    archive = OutArchive(value)
    shape_size = archive.get_size()
    shape = []
    for _ in range(shape_size):
        shape.append(archive.get_size())
    dtype = _context_protocol_to_numpy_dtype(archive.get_int())
    array_size = archive.get_size()
    check_argument(array_size == np.prod(shape))
    if dtype is object:
        data_copy = []
        for _ in range(array_size):
            data_copy.append(archive.get_string())
        if shape and shape[0] > 1:
            array = np.reshape(data_copy, shape)
        else:
            array = np.array(data_copy, dtype=dtype)
    else:
        array = np.ndarray(
            shape=shape,
            dtype=dtype,
            buffer=archive.get_block(array_size * dtype.itemsize),
            order="C",
        )
    return array


def decode_dataframe(value):
    if not value:
        raise RuntimeError("Value to decode should not be empty")
    archive = OutArchive(value)
    column_num = archive.get_size()
    row_num = archive.get_size()
    arrays = {}

    for _ in range(column_num):
        col_name = archive.get_string()
        dtype = _context_protocol_to_numpy_dtype(archive.get_int())
        if dtype is object:
            data_copy = []
            for _ in range(row_num):
                data_copy.append(archive.get_string())
            array = np.array(data_copy, dtype=dtype)
        else:
            array = np.ndarray(
                shape=(row_num,),
                dtype=dtype,
                buffer=archive.get_block(row_num * dtype.itemsize),
            )
        arrays[col_name] = array
    return pd.DataFrame(arrays)


def _unify_str_type(t):
    t = t.lower()
    if t in ("b", "bool"):
        return graph_def_pb2.DataTypePb.BOOL
    elif t in ("c", "char"):
        return graph_def_pb2.DataTypePb.CHAR
    elif t in ("s", "short"):
        return graph_def_pb2.DataTypePb.SHORT
    elif t in ("i", "int", "int32", "int32_t"):
        return graph_def_pb2.DataTypePb.INT
    elif t in ("l", "long", "int64_t", "int64"):
        return graph_def_pb2.DataTypePb.LONG
    elif t in ("uint32_t", "uint32"):
        return graph_def_pb2.DataTypePb.UINT
    elif t in ("uint64_t", "uint64"):
        return graph_def_pb2.DataTypePb.ULONG
    elif t in ("f", "float"):
        return graph_def_pb2.DataTypePb.FLOAT
    elif t in ("d", "double"):
        return graph_def_pb2.DataTypePb.DOUBLE
    elif t in ("str", "string", "std::string"):
        return graph_def_pb2.DataTypePb.STRING
    elif t == "bytes":
        return graph_def_pb2.DataTypePb.BYTES
    elif t == "int_list":
        return graph_def_pb2.DataTypePb.INT_LIST
    elif t == "long_list":
        return graph_def_pb2.DataTypePb.LONG_LIST
    elif t == "float_list":
        return graph_def_pb2.DataTypePb.FLOAT_LIST
    elif t == "double_list":
        return graph_def_pb2.DataTypePb.DOUBLE_LIST
    elif t in ("empty", "grape::emptytype"):
        return graph_def_pb2.NULLVALUE
    raise TypeError("Not supported type {}".format(t))


def unify_type(t):
    # If type is None, we deduce type from source file.
    if t is None:
        return graph_def_pb2.DataTypePb.UNKNOWN
    if isinstance(t, str):
        return _unify_str_type(t)
    elif isinstance(t, type):
        unify_types = {
            int: graph_def_pb2.LONG,
            np.int32: graph_def_pb2.INT,
            np.int64: graph_def_pb2.LONG,
            np.uint32: graph_def_pb2.UINT,
            np.uint64: graph_def_pb2.ULONG,
            float: graph_def_pb2.DOUBLE,
            np.float32: graph_def_pb2.FLOAT,
            np.float64: graph_def_pb2.DOUBLE,
            str: graph_def_pb2.STRING,
            np.str_: graph_def_pb2.STRING,
            bool: graph_def_pb2.BOOL,
            np.bool8: graph_def_pb2.BOOL,
            list: graph_def_pb2.INT_LIST,
            tuple: graph_def_pb2.INT_LIST,
            dict: graph_def_pb2.DYNAMIC,
        }
        return unify_types[t]
    elif isinstance(t, int):  # graph_def_pb2.DataType
        return t
    raise TypeError("Not supported type {}".format(t))


def data_type_to_cpp(t):
    if t == graph_def_pb2.INT:
        return "int32_t"
    elif t == graph_def_pb2.LONG:
        return "int64_t"
    elif t == graph_def_pb2.UINT:
        return "uint32_t"
    elif t == graph_def_pb2.ULONG:
        return "uint64_t"
    elif t == graph_def_pb2.FLOAT:
        return "float"
    elif t == graph_def_pb2.DOUBLE:
        return "double"
    elif t == graph_def_pb2.STRING:
        return "std::string"
    elif t is None or t == graph_def_pb2.NULLVALUE:
        return "grape::EmptyType"
    elif t == graph_def_pb2.DYNAMIC:
        return "dynamic::Value"
    elif t == graph_def_pb2.UNKNOWN:
        return ""
    raise ValueError("Not support type {}".format(t))


def data_type_to_python(t):
    if t in (
        graph_def_pb2.INT,
        graph_def_pb2.LONG,
        graph_def_pb2.UINT,
        graph_def_pb2.ULONG,
    ):
        return int
    elif t in (graph_def_pb2.FLOAT, graph_def_pb2.DOUBLE):
        return float
    elif t == graph_def_pb2.STRING:
        return str
    elif t in (None, graph_def_pb2.NULLVALUE):
        return None
    raise ValueError("Not support type {}".format(t))


def normalize_data_type_str(data_type):
    data_type = data_type.lower()
    if data_type in ("int8", "int8_t"):
        return "int8_t"
    if data_type in ("int16", "int16_t"):
        return "int16_t"
    if data_type in ("int", "int32_t", "int32"):
        return "int32_t"
    elif data_type in ("long", "int64_t", "int64"):
        return "int64_t"
    elif data_type in ("uint32_t", "uint32"):
        return "uint32_t"
    elif data_type in ("uint64_t", "uint64"):
        return "uint64_t"
    elif data_type in ("str", "string", "std::string"):
        return "std::string"
    else:
        return data_type


def vertex_map_type_to_enum(vertex_map):
    if isinstance(vertex_map, str):
        if vertex_map == "global":
            vertex_map = graph_def_pb2.GLOBAL_VERTEX_MAP
        elif vertex_map == "local":
            vertex_map = graph_def_pb2.LOCAL_VERTEX_MAP
        else:
            raise ValueError("vertex_map can only be global or local.")
    elif isinstance(vertex_map, int):
        assert vertex_map in (
            graph_def_pb2.GLOBAL_VERTEX_MAP,
            graph_def_pb2.LOCAL_VERTEX_MAP,
        )
    return vertex_map


def vertex_map_type_to_cpp(t):
    if t == graph_def_pb2.GLOBAL_VERTEX_MAP:
        return "vineyard::GlobalVertexMap"
    elif t == graph_def_pb2.LOCAL_VERTEX_MAP:
        return "vineyard::LocalVertexMap"
    else:
        raise ValueError("Not support vertex map type {}".format(t))


def transform_vertex_range(vertex_range):
    if vertex_range:
        return json.dumps(vertex_range)
    return None


def _from_numpy_dtype(dtype):
    dtype_reverse_map = {
        np.dtype(np.int8): types_pb2.INT8,
        np.dtype(np.int16): types_pb2.INT16,
        np.dtype(np.int32): types_pb2.INT32,
        np.dtype(np.int64): types_pb2.INT64,
        np.dtype(np.uint8): types_pb2.UINT8,
        np.dtype(np.uint16): types_pb2.UINT16,
        np.dtype(np.uint32): types_pb2.UINT32,
        np.dtype(np.uint64): types_pb2.UINT64,
        np.dtype(np.intc): types_pb2.INT,
        np.dtype(numpy_long): types_pb2.LONG,
        np.dtype(bool): types_pb2.BOOLEAN,
        np.dtype(float): types_pb2.FLOAT,
        np.dtype(np.double): types_pb2.DOUBLE,
        np.dtype(numpy_object): types_pb2.STRING,
    }
    pbdtype = dtype_reverse_map.get(dtype)
    if pbdtype is None:
        raise NotImplementedError("Do not support type {}".format(dtype))
    return pbdtype


def _to_numpy_dtype(dtype):
    dtype_map = {
        types_pb2.INT8: np.int8,
        types_pb2.INT16: np.int16,
        types_pb2.INT32: np.int32,
        types_pb2.INT64: np.int64,
        types_pb2.UINT8: np.uint8,
        types_pb2.UINT16: np.uint16,
        types_pb2.UINT32: np.uint32,
        types_pb2.UINT64: np.uint64,
        types_pb2.INT: np.intc,
        types_pb2.LONG: numpy_long,
        types_pb2.BOOLEAN: np.bool,
        types_pb2.FLOAT: np.float,
        types_pb2.DOUBLE: np.double,
        types_pb2.STRING: numpy_object,
    }
    npdtype = dtype_map.get(dtype)
    if npdtype is None:
        raise NotImplementedError("Do not support type {}".format(dtype))
    return npdtype

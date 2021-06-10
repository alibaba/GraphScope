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
import random
import string

import numpy as np
import pandas as pd
from google.protobuf.any_pb2 import Any

from graphscope.client.archive import OutArchive
from graphscope.framework.errors import check_argument
from graphscope.proto import attr_value_pb2
from graphscope.proto import data_types_pb2
from graphscope.proto import graph_def_pb2
from graphscope.proto import types_pb2


def random_string(nlen):
    """Create random string which length is `nlen`."""
    return "".join([random.choice(string.ascii_lowercase) for _ in range(nlen)])


def read_file_to_bytes(file_path):
    abs_dir = os.path.abspath(os.path.expanduser(file_path))
    if os.path.isfile(abs_dir):
        with open(abs_dir, "rb") as b:
            content = b.read()
        return content
    else:
        raise IOError("No such file: " + file_path)


def i_to_attr(i: int) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(i, int))
    return attr_value_pb2.AttrValue(i=i)


def b_to_attr(b: bool) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(b, bool))
    return attr_value_pb2.AttrValue(b=b)


def s_to_attr(s: str) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(s, str))
    return attr_value_pb2.AttrValue(s=s.encode("utf-8"))


def bytes_to_attr(s: bytes) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(s, bytes))
    return attr_value_pb2.AttrValue(s=s)


def f_to_attr(f: float) -> attr_value_pb2.AttrValue:
    check_argument(isinstance(f, float))
    return attr_value_pb2.AttrValue(f=f)


def type_to_attr(t):
    return attr_value_pb2.AttrValue(type=t)


def graph_type_to_attr(t):
    return attr_value_pb2.AttrValue(graph_type=t)


def modify_type_to_attr(t):
    return attr_value_pb2.AttrValue(modify_type=t)


def report_type_to_attr(t):
    return attr_value_pb2.AttrValue(report_type=t)


def place_holder_to_attr():
    return attr_value_pb2.AttrValue(place_holder=types_pb2.PlaceHolder())


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
    for k, v in kwargs.items():
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
    for i in range(shape_size):
        shape.append(archive.get_size())
    dtype = _context_protocol_to_numpy_dtype(archive.get_int())
    array_size = archive.get_size()
    check_argument(array_size == np.prod(shape))
    if dtype is object:
        data_copy = []
        for i in range(array_size):
            data_copy.append(archive.get_string())
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

    for i in range(column_num):
        col_name = archive.get_string()
        dtype = _context_protocol_to_numpy_dtype(archive.get_int())
        if dtype is object:
            data_copy = []
            for i in range(row_num):
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
            float: graph_def_pb2.DOUBLE,
            str: graph_def_pb2.STRING,
            bool: graph_def_pb2.BOOL,
            list: graph_def_pb2.INT_LIST,
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
        return "folly::dynamic"
    elif t == graph_def_pb2.UNKNOWN:
        return ""
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


def _transform_vertex_data_v(selector):
    if selector not in ("v.id", "v.data"):
        raise SyntaxError("selector of v must be 'id' or 'data'")
    return selector


def _transform_vertex_data_e(selector):
    if selector not in ("e.src", "e.dst", "e.data"):
        raise SyntaxError("selector of e must be 'src', 'dst' or 'data'")
    return selector


def _transform_vertex_data_r(selector):
    if selector != "r":
        raise SyntaxError("selector or r must be 'r'")
    return selector


def _transform_vertex_property_data_r(selector):
    # The second part of selector or r is user defined name.
    # So we will allow any str
    return selector


def _transform_labeled_vertex_data_v(schema, label, prop):
    label_id = schema.get_vertex_label_id(label)
    if prop == "id":
        return "label{}.{}".format(label_id, prop)
    else:
        prop_id = schema.get_vertex_property_id(label, prop)
        return "label{}.property{}".format(label_id, prop_id)


def _transform_labeled_vertex_data_e(schema, label, prop):
    label_id = schema.get_edge_label_id(label)
    if prop in ("src", "dst"):
        return "label{}.{}".format(label_id, prop)
    else:
        prop_id = schema.get_vertex_property_id(label, prop)
        return "label{}.property{}".format(label_id, prop_id)


def _transform_labeled_vertex_data_r(schema, label):
    label_id = schema.get_vertex_label_id(label)
    return "label{}".format(label_id)


def _transform_labeled_vertex_property_data_r(schema, label, prop):
    label_id = schema.get_vertex_label_id(label)
    return "label{}.{}".format(label_id, prop)


def transform_vertex_data_selector(selector):
    if selector is None:
        raise RuntimeError("selector cannot be None")
    segments = selector.split(".")
    if len(segments) > 2:
        raise SyntaxError("Invalid selector: %s." % selector)
    if segments[0] == "v":
        selector = _transform_vertex_data_v(selector)
    elif segments[0] == "e":
        selector = _transform_vertex_data_e(selector)
    elif segments[0] == "r":
        selector = _transform_vertex_data_r(selector)
    else:
        raise SyntaxError("Invalid selector: %s, choose from v / e / r." % selector)
    return selector


def transform_vertex_property_data_selector(selector):
    if selector is None:
        raise RuntimeError("selector cannot be None")
    segments = selector.split(".")
    if len(segments) != 2:
        raise SyntaxError("Invalid selector: %s." % selector)
    if segments[0] == "v":
        selector = _transform_vertex_data_v(selector)
    elif segments[0] == "e":
        selector = _transform_vertex_data_e(selector)
    elif segments[0] == "r":
        selector = _transform_vertex_property_data_r(selector)
    else:
        raise SyntaxError("Invalid selector: %s, choose from v / e / r." % selector)
    return selector


def transform_labeled_vertex_data_selector(schema, selector):
    """Formats: 'v:x.y/id', 'e:x.y/src/dst', 'r:label',
                x denotes label name, y denotes property name.
    Returned selector will change label name to 'label{id}', where id is x's id in labels.
    And change property name to 'property{id}', where id is y's id in properties.
    """
    if selector is None:
        raise RuntimeError("selector cannot be None")

    ret_type, segments = selector.split(":")
    if ret_type not in ("v", "e", "r"):
        raise SyntaxError("Invalid selector: " + selector)
    segments = segments.split(".")
    ret = ""
    if ret_type == "v":
        ret = _transform_labeled_vertex_data_v(schema, *segments)
    elif ret_type == "e":
        ret = _transform_labeled_vertex_data_e(schema, *segments)
    elif ret_type == "r":
        ret = _transform_labeled_vertex_data_r(schema, *segments)
    return "{}:{}".format(ret_type, ret)


def transform_labeled_vertex_property_data_selector(schema, selector):
    """Formats: 'v:x.y/id', 'e:x.y/src/dst', 'r:x.y',
                x denotes label name, y denotes property name.
    Returned selector will change label name to 'label{id}', where id is x's id in labels.
    And change property name to 'property{id}', where id is y's id in properties.
    """
    if selector is None:
        raise RuntimeError("selector cannot be None")
    ret_type, segments = selector.split(":")
    if ret_type not in ("v", "e", "r"):
        raise SyntaxError("Invalid selector: " + selector)
    segments = segments.split(".")
    ret = ""
    if ret_type == "v":
        ret = _transform_labeled_vertex_data_v(schema, *segments)
    elif ret_type == "e":
        ret = _transform_labeled_vertex_data_e(schema, *segments)
    elif ret_type == "r":
        ret = _transform_labeled_vertex_property_data_r(schema, *segments)
    return "{}:{}".format(ret_type, ret)


def transform_vertex_range(vertex_range):
    if vertex_range:
        return json.dumps(vertex_range)
    else:
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
        np.dtype(np.long): types_pb2.LONG,
        np.dtype(np.bool): types_pb2.BOOLEAN,
        np.dtype(np.float): types_pb2.FLOAT,
        np.dtype(np.double): types_pb2.DOUBLE,
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
        types_pb2.LONG: np.long,
        types_pb2.BOOLEAN: np.bool,
        types_pb2.FLOAT: np.float,
        types_pb2.DOUBLE: np.double,
    }
    npdtype = dtype_map.get(dtype)
    if npdtype is None:
        raise NotImplementedError("Do not support type {}".format(dtype))
    return npdtype

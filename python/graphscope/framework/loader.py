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

import logging
from typing import Dict
from typing import Sequence
from typing import Tuple
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from graphscope.client.session import get_default_session
from graphscope.framework import utils
from graphscope.framework.errors import check_argument
from graphscope.framework.vineyard_object import VineyardObject
from graphscope.proto import attr_value_pb2
from graphscope.proto import types_pb2

try:
    import vineyard
    import vineyard.io
except ImportError:
    vineyard = None

logger = logging.getLogger("graphscope")


class CSVOptions(object):
    """Options to read from CSV files.
    Avaiable options are:
        - column delimiters
        - include a subset of columns
        - types of each columns
        - whether the file contains a header
    """

    def __init__(self) -> None:
        # Field delimiter
        self.delimiter = ","

        # If non-empty, indicates the names of columns from the CSV file that should
        # be actually read and converted (in the list's order).
        # Columns not in this list will be ignored.
        self.include_columns = []
        # Optional per-column types (disabling type inference on those columns)
        self.column_types = []
        # include_columns always contains id column for v, src id and dst id column for e
        # if it contains and only contains those id columns, we suppose user actually want to
        # read all other properties. (Otherwise they should specify at least one property)
        self.force_include_all = False

        # If true, column names will be read from the first CSV row
        # If false, column names will be of the form "f0", "f1"...
        self.header_row = True

    def to_dict(self) -> Dict:
        options = {}
        options["delimiter"] = self.delimiter
        options["header_row"] = self.header_row
        if self.include_columns:
            options["schema"] = ",".join(self.include_columns)
        if self.column_types:
            cpp_types = [utils.data_type_to_cpp(dt) for dt in self.column_types]
            options["column_types"] = ",".join(cpp_types)
        if self.force_include_all:
            options["include_all_columns"] = self.force_include_all
        return options

    def __str__(self) -> str:
        return "&".join(["{}={}".format(k, v) for k, v in self.to_dict().items()])

    def __repr__(self) -> str:
        return self.__str__()


class Loader(object):
    """Generic data source wrapper.
    Loader can take various data sources, and assemble necessary information into a AttrValue.
    """

    def __init__(self, source, delimiter=",", header_row=True, **kwargs):
        """Initialize a loader with configurable options.
        Note: Loader cannot be reused since it may change inner state when constructing
        information for loading a graph.
        Args:
            source (str or value):
                The data source to be load, which could be one of the followings:

                    * local file: specified by URL :code:`file://...`
                    * oss file: specified by URL :code:`oss://...`
                    * hdfs file: specified by URL :code:`hdfs://...`
                    * s3 file: specified by URL :code:`s3://...`
                    * numpy ndarray, in CSR format
                    * pandas dataframe

                Ordinary data sources can be loaded using vineyard stream as well, a :code:`vineyard://`
                prefix can be used in the URL then the local file, oss object or HDFS file will be loaded
                into a vineyard stream first, then GraphScope's fragment will be built upon those streams
                in vineyard.

                Once the stream IO in vineyard reaches a stable state, it will be the default mode to
                load data sources and construct fragments in GraphScope.

            delimiter (char, optional): Column delimiter. Defaults to ','

            header_row (bool, optional): Whether source have a header. If true, column names
                will be read from the first row of source, else they are named by 'f0', 'f1', ....
                Defaults to True.

        Notes:
            Data is resolved by drivers in `libvineyard <https://github.com/alibaba/libvineyard>`_ .
            See more additional info in `Loading Graph` section of Docs, and implementations in `libvineyard`.
        """
        self.protocol = ""
        self.source = ""

        # options for data source is csv
        self.options = CSVOptions()
        check_argument(
            isinstance(delimiter, str) and len(delimiter) == 1,
            "The delimiter must be a single charactor, cannot be '%s'" % delimiter,
        )
        self.options.delimiter = delimiter
        self.options.header_row = header_row

        self.finished = False
        # metas for data source is numpy or dataframe
        self.row_num = 0
        self.column_num = 0
        self.deduced_properties = None
        self.property_bytes = None

        # extra args directly passed to storage system
        self.storage_options = kwargs

        # Allow to defer some execution until `finish()`
        self.preprocessor = None

        self.resolve(source)

    def __str__(self) -> str:
        return "{}: {}".format(self.protocol, self.source)

    def __repr__(self) -> str:
        return self.__str__()

    def resolve(self, source):
        """Dispatch resolver based on type of souce.

        Args:
            source: Different data sources

        Raises:
            RuntimeError: If the source is a not supported type.
        """
        if isinstance(source, str):
            self.process_location(source)
        elif isinstance(source, pd.DataFrame):
            self.process_pandas(source)
        elif isinstance(source, VineyardObject):
            self.process_vy_object(source)
        elif vineyard is not None and isinstance(source, vineyard.ObjectID):
            self.process_vy_object(source)
        elif isinstance(source, Sequence):
            # Assume a list of numpy array are passed as COO matrix, with length >= 2.
            # Formats: [src_id, dst_id, prop_1, ..., prop_n]
            check_argument(all([isinstance(item, np.ndarray) for item in source]))
            self.process_numpy(source)
        else:
            raise RuntimeError("Not support source", source)

    def process_location(self, source):
        self.protocol = urlparse(source).scheme
        # If protocol is not set, use 'file' as default
        if not self.protocol:
            self.protocol = "file"
        check_argument(
            self.protocol in ("file", "hdfs", "hive", "oss", "s3", "vineyard")
        )
        if self.protocol == "file":
            self.source = source
        else:
            self.process_vineyard(source)

    def process_numpy(self, source: Sequence[np.ndarray]):
        self.protocol = "numpy"
        self.row_num = source[0].shape[0]
        self.column_num = len(source)

        # Only support a subset of data types.
        check_argument(source[0].dtype in (np.dtype("int64"), np.dtype("long")))
        for col in source:
            check_argument(
                col.dtype
                in (
                    np.dtype("int64"),
                    np.dtype("long"),
                    np.dtype("float64"),
                )
            )

        col_names = ["f%s" % i for i in range(self.column_num)]
        col_types = [utils._from_numpy_dtype(col.dtype) for col in source]

        self.deduced_properties = list(zip(col_names, col_types))
        self.property_bytes = [col.tobytes("F") for col in source]

    def process_pandas(self, source: pd.DataFrame):
        self.protocol = "pandas"
        check_argument(len(source.shape) == 2)
        self.row_num = source.shape[0]
        self.column_num = source.shape[1]

        # Only support a subset of data types.
        check_argument(source.dtypes.values[0] in (np.dtype("int64"), np.dtype("long")))
        for dtype in source.dtypes.values:
            check_argument(
                dtype in (np.dtype("int64"), np.dtype("long"), np.dtype("float64"))
            )

        col_names = list(source.columns.values)
        col_types = [utils._from_numpy_dtype(dtype) for dtype in source.dtypes.values]

        self.deduced_properties = list(zip(col_names, col_types))
        self.property_bytes = [source[name].values.tobytes("F") for name in col_names]

    def process_vineyard(self, source):
        if vineyard is None:
            raise RuntimeError("Vineyard is not installed")
        # defer execution of `vineyard.io.open` because `read_options` is unknown
        # until load_from has been fully processed.

        def func(source, storage_options, read_options, sess):
            info = sess.info
            vineyard_endpoint = info["engine_config"]["vineyard_rpc_endpoint"]
            vineyard_ipc_socket = info["engine_config"]["vineyard_socket"]
            hosts = info["engine_hosts"].split(",")
            if "namespace" in info:
                deployment = "kubernetes"
                hosts = ["%s:%s" % (info["namespace"], host) for host in hosts]
            else:
                deployment = "ssh"
            num_workers = info["num_workers"]

            stream_id = repr(
                vineyard.io.open(
                    source,
                    mode="r",
                    vineyard_endpoint=vineyard_endpoint,
                    vineyard_ipc_socket=vineyard_ipc_socket,
                    hosts=hosts,
                    num_workers=num_workers,
                    deployment=deployment,
                    read_options=read_options,
                    storage_options=storage_options,
                )
            )
            return "vineyard", stream_id

        self.source = source
        self.preprocessor = func

    def finish(self):
        if self.finished:
            return
        if self.preprocessor is not None:
            self.protocol, self.source = self.preprocessor(
                self.source,
                self.storage_options,
                self.options.to_dict(),
                get_default_session(),
            )
            logger.debug(
                f"processed protocol = {self.protocol}, source = {self.source}"
            )
        self.finished = True

    def process_vy_object(self, source):
        self.protocol = "vineyard"
        if isinstance(source, vineyard.ObjectID):
            self.source = repr(source)
        else:
            self.source = repr(source.object_id)

    def select_columns(self, columns: Sequence[Tuple[str, int]], include_all=False):
        for name, data_type in columns:
            self.options.include_columns.append(name)
            self.options.column_types.append(data_type)
        self.options.force_include_all = include_all

    def get_attr(self):
        if not self.finished:
            self.finish()
        attr = attr_value_pb2.AttrValue()
        attr.func.name = "loader"
        attr.func.attr[types_pb2.PROTOCOL].CopyFrom(utils.s_to_attr(self.protocol))
        # Let graphscope handle local files cause it's implemented in c++ and
        # doesn't add an additional stream layer.
        # Maybe handled by vineyard in the near future
        if self.protocol == "file":
            source = "{}#{}".format(self.source, self.options)
            attr.func.attr[types_pb2.VALUES].CopyFrom(
                utils.bytes_to_attr(source.encode("utf-8"))
            )
        elif self.protocol in ("numpy", "pandas"):
            attr.func.attr[types_pb2.ROW_NUM].CopyFrom(utils.i_to_attr(self.row_num))
            attr.func.attr[types_pb2.COLUMN_NUM].CopyFrom(
                utils.i_to_attr(self.column_num)
            )
            # Use key start from 10000 + col_index to store raw bytes.
            for i in range(len(self.property_bytes)):
                attr.func.attr[10000 + i].CopyFrom(
                    utils.bytes_to_attr(self.property_bytes[i])
                )
        else:  # Let vineyard handle other data source.
            attr.func.attr[types_pb2.VALUES].CopyFrom(
                utils.bytes_to_attr(self.source.encode("utf-8"))
            )
        return attr

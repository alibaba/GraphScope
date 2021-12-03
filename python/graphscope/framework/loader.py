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
import logging
import pathlib
from typing import Dict
from typing import Sequence
from typing import Tuple
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pyarrow as pa

from graphscope.framework import utils
from graphscope.framework.errors import check_argument
from graphscope.proto import attr_value_pb2
from graphscope.proto import types_pb2

try:
    import vineyard
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
        # For numpy or pandas, source is the serialized raw bytes
        # For files, it's the location
        # For vineyard, it's the ID or name
        self.source = ""
        # options for data source is csv
        self.options = CSVOptions()
        check_argument(
            isinstance(delimiter, str) and len(delimiter) == 1,
            "The delimiter must be a single charactor, cannot be '%s'" % delimiter,
        )
        self.options.delimiter = delimiter
        self.options.header_row = header_row
        # metas for data source is numpy or dataframe
        self.deduced_properties = None
        # extra args directly passed to storage system
        # find more details in fsspec
        #   https://filesystem-spec.readthedocs.io/en/latest/
        self.storage_options = kwargs
        # also parse protocol and source in `resolve` method
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
        elif isinstance(source, pathlib.Path):
            self.process_location(str(source))
        elif isinstance(source, pd.DataFrame):
            self.process_pandas(source)
        elif vineyard is not None and isinstance(
            source, (vineyard.Object, vineyard.ObjectID, vineyard.ObjectName)
        ):
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
        self.source = source

    def process_numpy(self, source: Sequence[np.ndarray]):
        """Transform arrays to equivalent DataFrame,
        note the transpose is necessary.
        """
        col_names = ["f%s" % i for i in range(len(source))]
        df = pd.DataFrame(source, col_names).T
        types = {}
        for i, _ in enumerate(source):
            types[col_names[i]] = source[i].dtype
        df = df.astype(types)
        return self.process_pandas(df)

    def process_pandas(self, source: pd.DataFrame):
        self.protocol = "pandas"
        col_names = list(source.columns.values)
        col_types = [utils._from_numpy_dtype(dtype) for dtype in source.dtypes.values]

        table = pa.Table.from_pandas(source, preserve_index=False)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        buf = sink.getvalue()

        self.deduced_properties = list(zip(col_names, col_types))
        self.source = bytes(memoryview(buf))

    def process_vy_object(self, source):
        self.protocol = "vineyard"
        # encoding: add a `o` prefix to object id, and a `s` prefix to object name.
        if isinstance(source, vineyard.Object):
            self.source = "o%s" % repr(source.id)
        elif isinstance(source, vineyard.ObjectID):
            self.source = "o%s" % repr(source)
        elif isinstance(source, vineyard.ObjectName):
            self.source = "s%s" % str(source)
        else:
            raise ValueError(
                "Invalid input source: not a vineyard's Object, ObjectID or ObjectName"
            )

    def select_columns(self, columns: Sequence[Tuple[str, int]], include_all=False):
        self.options.include_columns = []
        self.options.column_types = []
        for name, data_type in columns:
            self.options.include_columns.append(name)
            self.options.column_types.append(data_type)
        self.options.force_include_all = include_all

    def get_attr(self):
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
        elif self.protocol == "pandas":
            attr.func.attr[types_pb2.VALUES].CopyFrom(utils.bytes_to_attr(self.source))
        else:  # Let vineyard handle other data source.
            attr.func.attr[types_pb2.VALUES].CopyFrom(
                utils.bytes_to_attr(self.source.encode("utf-8"))
            )
            if self.protocol != "vineyard":
                # need spawn an io stream in coordinator
                attr.func.attr[types_pb2.STORAGE_OPTIONS].CopyFrom(
                    utils.s_to_attr(json.dumps(self.storage_options))
                )
                attr.func.attr[types_pb2.READ_OPTIONS].CopyFrom(
                    utils.s_to_attr(json.dumps(self.options.to_dict()))
                )
        return attr

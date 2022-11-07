/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.utils;

public class CppClassName {

    public static final String VINEYARD_CLIENT = "vineyard::Client";
    public static final String ARROW_FRAGMENT = "gs::ArrowFragmentDefault";
    public static final String ARROW_FRAGMENT_GROUP = "vineyard::ArrowFragmentGroup";
    public static final String PROPERTY_MESSAGE_MANAGER = "gs::PropertyMessageManager";

    public static final String EDGE_DATA_COLUMN = "gs::EdgeDataColumnDefault";
    public static final String VERTEX_DATA_COLUMN = "gs::VertexDataColumnDefault";
    public static final String PROPERTY_ADJ_LIST = "gs::AdjListDefault";
    public static final String PROPERTY_NBR = "gs::NbrDefault";
    public static final String PROPERTY_NBR_UNIT = "gs::NbrUnitDefault";
    public static final String PROPERTY_RAW_ADJ_LIST = "gs::RawAdjListDefault";

    public static final String I_COLUMN = "gs::IColumn";
    public static final String DOUBLE_COLUMN = "gs::DoubleColumn";
    public static final String LONG_COLUMN = "gs::LongColumn";
    public static final String INT_COLUMN = "gs::IntColumn";
    public static final String LABELED_VERTEX_PROPERTY_CONTEXT = "gs::LabeledVertexPropertyContext";
    public static final String VERTEX_PROPERTY_CONTEXT = "gs::VertexPropertyContext";

    public static final String LABELED_VERTEX_DATA_CONTEXT = "gs::LabeledVertexDataContext";
    public static final String VERTEX_DATA_CONTEXT = "grape::VertexDataContext";
    public static final String CONTEXT_DATA_TYPE = "gs::ContextDataType";

    public static final String DOUBLE_MSG = "gs::DoubleMsg";
    public static final String LONG_MSG = "gs::LongMsg";
    public static final String INT_MSG = "gs::PrimitiveMessage<int32_t>";
    public static final String GS_PRIMITIVE_MESSAGE = "gs::PrimitiveMessage";
    public static final String STRING_MSG = "gs::StringMsg";
    public static final String GRAPE_ADJ_LIST = "grape::AdjList";
    public static final String GRAPE_BIT_SET = "grape::Bitset";
    public static final String GRAPE_DENSE_VERTEX_SET = "grape::DenseVertexSet";
    public static final String GRAPE_DEST_LIST = "grape::DestList";
    public static final String GRAPE_EMPTY_TYPE = "grape::EmptyType";
    public static final String GRAPE_NBR = "grape::Nbr";
    public static final String GRAPE_VERTEX = "grape::Vertex";
    public static final String GRAPE_LONG_VERTEX = "grape::Vertex<uint64_t>";
    public static final String GRAPE_VERTEX_ARRAY = "gs::JavaVertexArray";
    public static final String GRAPE_VERTEX_RANGE = "grape::VertexRange";
    public static final String GRAPE_IMMUTABLE_FRAGMENT = "grape::ImmutableEdgecutFragment";
    public static final String GRAPE_DEFAULT_MESSAGE_MANAGER = "grape::DefaultMessageManager";
    public static final String GRAPE_MESSAGE_IN_BUFFER = "grape::MessageInBuffer";
    public static final String GRAPE_PARALLEL_MESSAGE_MANAGER = "grape::ParallelMessageManager";
    public static final String GRAPE_PARALLEL_THREAD_LOCAL_MESSAGE_BUFFER =
            "grape::ThreadLocalMessageBuffer";
    public static final String GS_PARALLEL_PROPERTY_MESSAGE_MANAGER =
            "gs::ParallelPropertyMessageManager";
    public static final String GRAPE_COMMUNICATOR = "grape::Communicator";

    public static final String CPP_ARROW_PROJECTED_FRAGMENT = "gs::ArrowProjectedFragment";

    public static final String CPP_ARROW_PROJECTED_FRAGMENT_MAPPER =
            "gs::ArrowProjectedFragmentMapper";

    public static final String CPP_ARROW_PROJECTED_FRAGMENT_GETTER =
            "gs::ArrowProjectedFragmentGetter";

    public static final String PROJECTED_ADJ_LIST =
            "gs::arrow_projected_fragment_impl::AdjListDefault";
    public static final String PROJECTED_NBR = "gs::arrow_projected_fragment_impl::NbrDefault";

    public static final String STD_UNORDERED_MAP = "std::unordered_map";
    public static final String GS_VERTEX_ARRAY = "gs::VertexArrayDefault";
    public static final String GRAPE_IN_ARCHIVE = "grape::InArchive";
    public static final String GRAPE_OUT_ARCHIVE = "grape::OutArchive";

    public static final String GS_ARROW_PROJECTED_FRAGMENT_IMPL_TYPED_ARRAY =
            "gs::arrow_projected_fragment_impl::TypedArray";
    public static final String GS_ARROW_PROJECTED_FRAGMENT_IMPL_STRING_TYPED_ARRAY =
            "gs::arrow_projected_fragment_impl::TypedArray<std::string>";

    public static final String GS_GRAPHX_RAW_DATA = "gs::GraphXRawData";
    public static final String GS_GRAPHX_RAW_DATA_BUILDER = "gs::GraphXRawDataBuilder";
    public static final String GS_DEFAULT_IMMUTABLE_CSR = "gs::DefaultImmutableCSR";
    public static final String GS_ARROW_ARRAY_BUILDER = "gs::ArrowArrayBuilder";
    public static final String GS_ARROW_STRING_ARRAY_BUILDER = "gs::ArrowArrayBuilder<std::string>";
    public static final String GS_ARROW_ARRAY = "gs::ArrowArray";
    public static final String GS_ARROW_STRING_ARRAY = "gs::ArrowArray<std::string>";

    public static final String ARROW_STATUS = "arrow::Status";
    public static final String VINEYARD_STATUS = "vineyard::Status";
    public static final String VINEYARD_ARRAY_BUILDER = "vineyard::ArrayBuilder";

    public static final String ARROW_FRAGMENT_GROUP_GETTER = "gs::ArrowFragmentGroupGetter";
    public static final String VINEYARD_JSON = "vineyard::json";

    public static final String CXX_STD_STRING = "com.alibaba.fastffi.impl.CXXStdString";
}

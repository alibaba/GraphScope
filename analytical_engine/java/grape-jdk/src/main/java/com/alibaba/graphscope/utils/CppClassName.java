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
    public static final String GRAPE_VERTEX_ARRAY = "grape::VertexArray";
    public static final String GRAPE_VERTEX_RANGE = "grape::VertexRange";
    public static final String GRAPE_IMMUTABLE_FRAGMENT = "grape::ImmutableEdgecutFragment";
    public static final String GRAPE_DEFAULT_MESSAGE_MANAGER = "grape::DefaultMessageManager";
    public static final String GRAPE_MESSAGE_IN_BUFFER = "grape::MessageInBuffer";
    public static final String GRAPE_PARALLEL_MESSAGE_MANAGER = "grape::ParallelMessageManager";
    public static final String GS_PARALLEL_PROPERTY_MESSAGE_MANAGER =
            "gs::ParallelPropertyMessageManager";
    public static final String GRAPE_COMMUNICATOR = "grape::Communicator";
    public static final String ARROW_PROJECTED_FRAGMENT = "gs::ArrowProjectedFragment";
    public static final String PROJECTED_ADJ_LIST =
            "gs::arrow_projected_fragment_impl::AdjListDefault";
    public static final String PROJECTED_NBR = "gs::arrow_projected_fragment_impl::NbrDefault";

    public static final String STD_UNORDERED_MAP = "std::unordered_map";
    public static final String GS_VERTEX_ARRAY = "gs::VertexArrayDefault";
}

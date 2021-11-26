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

public class CppHeaderName {
    public static final String ARROW_FRAGMENT_H = "vineyard/graph/fragment/arrow_fragment.h";
    public static final String ARROW_FRAGMENT_GROUP_H =
            "vineyard/graph/fragment/arrow_fragment_group.h";
    public static final String PROPERTY_MESSAGE_MANAGER_H =
            "core/parallel/property_message_manager.h";
    public static final String PROPERTY_GRAPH_UTILS_H =
            "vineyard/graph/fragment/property_graph_utils.h";
    public static final String CORE_JAVA_TYPE_ALIAS_H = "core/java/type_alias.h";
    public static final String LABELED_VERTEX_PROPERTY_CONTEXT_H =
            "core/context/labeled_vertex_property_context.h";
    public static final String VERTEX_PROPERTY_CONTEXT_H = "core/context/vertex_property_context.h";

    public static final String LABELED_VERTEX_DATA_CONTEXT_H = "core/context/vertex_data_context.h";
    public static final String VERTEX_DATA_CONTEXT_H = "grape/app/vertex_data_context.h";
    public static final String CONTEXT_PROTOCOLS_H = "core/context/context_protocols.h";

    public static final String GRAPE_ADJ_LIST_H = "grape/graph/adj_list.h";
    public static final String GRAPE_TYPES_H = "grape/types.h";
    public static final String GRAPE_BIT_SET_H = "grape/utils/bitset.h";
    public static final String GRAPE_DENSE_VERTEX_SET_H = "grape/utils/vertex_set.h";
    public static final String GRAPE_VERTEX_ARRAY_H = "grape/utils/vertex_array.h";
    public static final String GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H =
            "grape/fragment/immutable_edgecut_fragment.h";
    public static final String CORE_JAVA_JAVA_MESSAGES_H = "core/java/java_messages.h";
    public static final String GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H =
            "grape/parallel/message_in_buffer.h";
    public static final String GRAPE_PARALLEL_DEFAULT_MESSAGE_MANAGER_H =
            "grape/parallel/default_message_manager.h";
    public static final String GRAPE_PARALLEL_PARALLEL_MESSAGE_MANAGER_H =
            "grape/parallel/parallel_message_manager.h";
    public static final String CORE_PARALLEL_PARALLEL_PROPERTY_MESSAGE_MANAGER_H =
            "core/parallel/parallel_property_message_manager.h";
    // public static final String GRAPE_MESSAGE_IN_BUFFER_H =
    // "core/parallel/java_message_in_buffer.h";
    public static final String GRAPE_LONG_VECTOR_H = "core/utils/long_vector.h";
    // this header contains the self-defined-ds generate ffi cpp files,
    // this header should be include in message managers
    public static final String JAVA_APP_JNI_FFI_H = "java-app-jni-ffi.h";
    public static final String GS_CORE_CONFIG_H = "core/config.h";
    public static final String GRAPE_COMMUNICATOR_H = "grape/communication/communicator.h";
    public static final String ARROW_PROJECTED_FRAGMENT_H =
            "core/fragment/arrow_projected_fragment.h";
    // public static final String CORE_JAVA_TYPE_ALIAS_H = "core/java/type_alias.h";
    public static final String GRAPE_WORKER_COMM_SPEC_H = "grape/worker/comm_spec.h";
}

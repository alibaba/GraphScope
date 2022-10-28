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

package com.alibaba.graphscope.annotation;

import static com.alibaba.graphscope.utils.CppClassName.ARROW_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_STRING_VED_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.DOUBLE_MSG;
import static com.alibaba.graphscope.utils.CppClassName.GS_PRIMITIVE_MESSAGE;
import static com.alibaba.graphscope.utils.CppClassName.GS_VERTEX_ARRAY;
import static com.alibaba.graphscope.utils.CppClassName.LONG_MSG;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_FRAGMENT_GROUP_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_GRAPHX_RAW_DATA_H;
import static com.alibaba.graphscope.utils.CppHeaderName.VINEYARD_JSON_H;
import static com.alibaba.graphscope.utils.JavaClassName.DOUBLE;
import static com.alibaba.graphscope.utils.JavaClassName.INTEGER;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_FRAGMENT_GETTER;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_FRAGMENT_MAPPER;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT_GETTER;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT_MAPPER;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT_GETTER;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT_MAPPER;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_VED_FRAGMENT;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_VED_FRAGMENT_GETTER;
import static com.alibaba.graphscope.utils.JavaClassName.JAVA_ARROW_PROJECTED_STRING_VED_FRAGMENT_MAPPER;
import static com.alibaba.graphscope.utils.JavaClassName.LONG;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIFunGen;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGenBatch;
import com.alibaba.graphscope.utils.CppClassName;

/**
 * In <em>GRAPE-jdk</em>, we split programming interfaces from actual implementation. As the
 * programming interfaces are defined in grape-jdk, the actually implementation code generated with
 * annotation processors. Each FFIGen defines a generation schema for a FFIPointer interface.
 */
@FFIGenBatch(
        value = {
            @FFIGen(type = "com.alibaba.graphscope.ds.FidPointer"),
            @FFIGen(type = "com.alibaba.graphscope.ds.DestList"),
            @FFIGen(type = "com.alibaba.graphscope.stdcxx.CCharPointer"),
            @FFIGen(type = "com.alibaba.graphscope.ds.EmptyType"),
            @FFIGen(type = "com.alibaba.graphscope.parallel.message.DoubleMsg"),
            @FFIGen(type = "com.alibaba.graphscope.parallel.message.LongMsg"),
            @FFIGen(type = "com.alibaba.graphscope.parallel.message.IntMsg"),
            @FFIGen(type = "com.alibaba.graphscope.arrow.Status"),
            @FFIGen(
                    type = "com.alibaba.graphscope.stdcxx.StdUnorderedMap",
                    templates = {
                        @CXXTemplate(
                                cxx = {"unsigned", "uint64_t"},
                                java = {"java.lang.Integer", "java.lang.Long"})
                    }),
            @FFIGen(type = "com.alibaba.graphscope.fragment.ArrowFragmentGroup"),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.Vertex",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.PropertyNbrUnit",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer"),
                    }),
            @FFIGen(type = "com.alibaba.graphscope.ds.StringTypedArray"),
            @FFIGen(
                    library = "grape-jni",
                    type = "com.alibaba.graphscope.arrow.array.ArrowArrayBuilder",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                        @CXXTemplate(cxx = "double", java = "Double")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.GrapeNbr",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "double"},
                                java = {"Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int32_t"},
                                java = {"Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int64_t"},
                                java = {"Long", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.graphx.GraphXRawData",
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "int32_t"},
                                java = {"Long", "Long", "Integer", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int64_t"},
                                java = {"Long", "Long", "Long", "Long"}),
                    }),
            @FFIGen(type = "com.alibaba.graphscope.graphx.VineyardClient"),
            @FFIGen(type = "com.alibaba.graphscope.graphx.V6dStatus"),
            @FFIGen(type = "com.alibaba.graphscope.graphx.Json"),
            @FFIGen(
                    type = "com.alibaba.graphscope.stdcxx.StdMap",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "vineyard::json"},
                                java = {"java.lang.Long", "com.alibaba.graphscope.graphx.Json"},
                                include = @CXXHead(VINEYARD_JSON_H))
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.stdcxx.StdUnorderedMap",
                    templates = {
                        @CXXTemplate(
                                cxx = {"unsigned", "uint64_t"},
                                java = {"java.lang.Integer", "java.lang.Long"})
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.GSVertexArray",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "double", java = "Double"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.VertexRange",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.PropertyNbr",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.PropertyAdjList",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.PropertyRawAdjList",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.fragment.ArrowFragment",
                    templates = {@CXXTemplate(cxx = "int64_t", java = "Long")},
                    functionTemplates = {
                        @FFIFunGen(
                                name = "edgeDataColumn",
                                parameterTypes = {"DATA_T"},
                                returnType = "com.alibaba.graphscope.ds.EdgeDataColumn<DATA_T>",
                                templates = {
                                    @CXXTemplate(cxx = "int64_t", java = "Long"),
                                    @CXXTemplate(cxx = "double", java = "Double"),
                                    @CXXTemplate(cxx = "int32_t", java = "Integer")
                                })
                    }),
            @FFIGen(type = "com.alibaba.graphscope.column.IColumn"),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.EdgeDataColumn",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                        @CXXTemplate(cxx = "double", java = "Double")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.column.DoubleColumn",
                    templates = {
                        @CXXTemplate(
                                cxx = {ARROW_FRAGMENT + "<int64_t>"},
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,int64_t>"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,double,int64_t>"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>"
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.column.LongColumn",
                    templates = {
                        @CXXTemplate(
                                cxx = {ARROW_FRAGMENT + "<int64_t>"},
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,int64_t>"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,double,int64_t>"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>"
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.column.IntColumn",
                    templates = {
                        @CXXTemplate(
                                cxx = {ARROW_FRAGMENT + "<int64_t>"},
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,int64_t>"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,double,int64_t>"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>"
                                }),
                    }),
            @FFIGen(type = "com.alibaba.graphscope.fragment.ArrowFragmentGroup"),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.Vertex",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.PropertyNbrUnit",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.TypedArray",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                        @CXXTemplate(cxx = "double", java = "Double"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.ProjectedNbr",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "double"},
                                java = {"Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int32_t"},
                                java = {"Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int64_t"},
                                java = {"Long", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.ProjectedNbrStrData",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t"},
                                java = {"Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.ProjectedAdjList",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "double"},
                                java = {"Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int64_t"},
                                java = {"Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int32_t"},
                                java = {"Long", "Integer"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.ProjectedAdjListStrData",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t"},
                                java = {"Long"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_FRAGMENT,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int64_t"},
                                java = {"Long", "Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int32_t"},
                                java = {"Long", "Long", "Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "double"},
                                java = {"Long", "Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "int64_t"},
                                java = {"Long", "Long", "Integer", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "int32_t"},
                                java = {"Long", "Long", "Integer", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "double"},
                                java = {"Long", "Long", "Integer", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int64_t"},
                                java = {"Long", "Long", "Double", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int32_t"},
                                java = {"Long", "Long", "Double", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "double"},
                                java = {"Long", "Long", "Double", "Double"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t"},
                                java = {"Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double"},
                                java = {"Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t"},
                                java = {"Long", "Long", "Integer"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t"},
                                java = {"Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double"},
                                java = {"Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t"},
                                java = {"Long", "Long", "Integer"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_VED_FRAGMENT,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t"},
                                java = {"Long", "Long"}),
                    }),
            @FFIGen(type = "com.alibaba.graphscope.fragment.getter.ArrowFragmentGroupGetter"),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_FRAGMENT_GETTER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int64_t"},
                                java = {"Long", "Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int32_t"},
                                java = {"Long", "Long", "Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "double"},
                                java = {"Long", "Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "int64_t"},
                                java = {"Long", "Long", "Integer", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "int32_t"},
                                java = {"Long", "Long", "Integer", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "double"},
                                java = {"Long", "Long", "Integer", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int64_t"},
                                java = {"Long", "Long", "Double", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int32_t"},
                                java = {"Long", "Long", "Double", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "double"},
                                java = {"Long", "Long", "Double", "Double"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT_GETTER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t"},
                                java = {"Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double"},
                                java = {"Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t"},
                                java = {"Long", "Long", "Integer"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT_GETTER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t"},
                                java = {"Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double"},
                                java = {"Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t"},
                                java = {"Long", "Long", "Integer"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_VED_FRAGMENT_GETTER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t"},
                                java = {"Long", "Long"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_FRAGMENT_MAPPER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int64_t"},
                                java = {"Long", "Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int32_t"},
                                java = {"Long", "Long", "Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "double"},
                                java = {"Long", "Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "int64_t"},
                                java = {"Long", "Long", "Integer", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "int32_t"},
                                java = {"Long", "Long", "Integer", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "double"},
                                java = {"Long", "Long", "Integer", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int64_t"},
                                java = {"Long", "Long", "Double", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int32_t"},
                                java = {"Long", "Long", "Double", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "double"},
                                java = {"Long", "Long", "Double", "Double"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT_MAPPER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t"},
                                java = {"Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double"},
                                java = {"Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t"},
                                java = {"Long", "Long", "Integer"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT_MAPPER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t"},
                                java = {"Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double"},
                                java = {"Long", "Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t"},
                                java = {"Long", "Long", "Integer"}),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_STRING_VED_FRAGMENT_MAPPER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t"},
                                java = {"Long", "Long"}),
                    }),
            // mapper
            @FFIGen(
                    type = "com.alibaba.graphscope.stdcxx.StdSharedPtr",
                    templates = {
                        @CXXTemplate(
                                cxx = "gs::ArrowFragmentDefault<int64_t>",
                                java =
                                        "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>"),
                        @CXXTemplate(
                                cxx = "gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.DoubleColumn<com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>>"),
                        @CXXTemplate(
                                cxx = "gs::IntColumn<gs::ArrowFragmentDefault<int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.IntColumn<com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>>"),
                        @CXXTemplate(
                                cxx = "gs::LongColumn<gs::ArrowFragmentDefault<int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.LongColumn<com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        "gs::DoubleColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.DoubleColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        "gs::IntColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.IntColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        "gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.LongColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        "gs::DoubleColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.DoubleColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        "gs::IntColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.IntColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        "gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,double,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.LongColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,int64_t,int64_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + LONG
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,int64_t,int32_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + INTEGER
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,int64_t,double>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + DOUBLE
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                                + "<int64_t,uint64_t,int64_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + LONG
                                                + ">"),
                        // vd int32_t
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,int32_t,int64_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + INTEGER
                                                + ","
                                                + LONG
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,int32_t,int32_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + INTEGER
                                                + ","
                                                + INTEGER
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,int32_t,double>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + INTEGER
                                                + ","
                                                + DOUBLE
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                                + "<int64_t,uint64_t,int32_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + INTEGER
                                                + ">"),
                        // vd double
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,double,int64_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + DOUBLE
                                                + ","
                                                + LONG
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,double,int32_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + DOUBLE
                                                + ","
                                                + INTEGER
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_FRAGMENT
                                                + "<int64_t,uint64_t,double,double>",
                                java =
                                        JAVA_ARROW_PROJECTED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + DOUBLE
                                                + ","
                                                + DOUBLE
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                                + "<int64_t,uint64_t,double>",
                                java =
                                        JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + DOUBLE
                                                + ">"),
                        // vd std string
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                                + "<int64_t,uint64_t,int64_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + LONG
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                                + "<int64_t,uint64_t,int32_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + INTEGER
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                                + "<int64_t,uint64_t,double>",
                                java =
                                        JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ","
                                                + DOUBLE
                                                + ">"),
                        @CXXTemplate(
                                cxx =
                                        CPP_ARROW_PROJECTED_STRING_VED_FRAGMENT
                                                + "<int64_t,uint64_t>",
                                java =
                                        JAVA_ARROW_PROJECTED_STRING_VED_FRAGMENT
                                                + "<"
                                                + LONG
                                                + ","
                                                + LONG
                                                + ">"),
                        @CXXTemplate(
                                cxx = "vineyard::ArrowFragmentGroup",
                                java = "com.alibaba.graphscope.fragment.ArrowFragmentGroup",
                                include = {@CXXHead(ARROW_FRAGMENT_GROUP_H)}),
                        @CXXTemplate(
                                cxx = "gs::GraphXRawData<int64_t,uint64_t,int32_t,int32_t>",
                                java =
                                        "com.alibaba.graphscope.graphx.GraphXRawData<java.lang.Long,java.lang.Long,java.lang.Integer,java.lang.Integer>",
                                include = {@CXXHead(CORE_JAVA_GRAPHX_RAW_DATA_H)}),
                        @CXXTemplate(
                                cxx = "gs::GraphXRawData<int64_t,uint64_t,int64_t,int64_t>",
                                java =
                                        "com.alibaba.graphscope.graphx.GraphXRawData<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>",
                                include = {@CXXHead(CORE_JAVA_GRAPHX_RAW_DATA_H)})
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.graphx.GraphXRawDataBuilder",
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int32_t", "int32_t"},
                                java = {"Long", "Long", "Integer", "Integer"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int64_t"},
                                java = {"Long", "Long", "Long", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.graphx.VineyardArrayBuilder",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                        @CXXTemplate(cxx = "double", java = "Double")
                    }),
            @FFIGen(
                    library = "grape-jni",
                    type = "com.alibaba.graphscope.arrow.array.ArrowArrayBuilder",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                        @CXXTemplate(cxx = "double", java = "Double")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.GrapeNbr",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "double"},
                                java = {"Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int32_t"},
                                java = {"Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int64_t"},
                                java = {"Long", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.message.PrimitiveMessage",
                    templates = {
                        @CXXTemplate(cxx = "double", java = "Double"),
                        @CXXTemplate(cxx = "int64_t", java = "Long")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.VertexArray",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "int64_t"},
                                java = {"Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int32_t"},
                                java = {"Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "double"},
                                java = {"Long", "Double"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.GrapeAdjList",
                    templates = {
                        @CXXTemplate(
                                cxx = {"uint64_t", "double"},
                                java = {"Long", "Double"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int32_t"},
                                java = {"Long", "Integer"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "int64_t"},
                                java = {"Long", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.stdcxx.StdVector",
                    templates = {
                        @CXXTemplate(cxx = "char", java = "Byte"),
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "double", java = "Double"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                        @CXXTemplate(
                                cxx = "grape::Vertex<uint64_t>",
                                java = "com.alibaba.graphscope.ds.Vertex<java.lang.Long>"),
                        @CXXTemplate(
                                cxx = GS_VERTEX_ARRAY + "<double>",
                                java = "com.alibaba.graphscope.ds.GSVertexArray" + "<Double>"),
                        @CXXTemplate(
                                cxx = GS_VERTEX_ARRAY + "<int32_t>",
                                java = "com.alibaba.graphscope.ds.GSVertexArray" + "<Integer>"),
                        @CXXTemplate(
                                cxx = GS_VERTEX_ARRAY + "<int64_t>",
                                java =
                                        "com.alibaba.graphscope.ds.GSVertexArray"
                                                + "<java.lang.Long>"),
                    }),
            @FFIGen(
                    type = JAVA_ARROW_PROJECTED_FRAGMENT_MAPPER,
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "int64_t", "int64_t"},
                                java = {"Long", "Long", "Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int64_t"},
                                java = {"Long", "Long", "Double", "Long"}),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int32_t"},
                                java = {"Long", "Long", "Double", "Integer"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.context.ffi.FFILabeledVertexDataContext",
                    templates = {
                        @CXXTemplate(
                                cxx = {ARROW_FRAGMENT + "<int64_t>", "double"},
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                    "Double"
                                }),
                        @CXXTemplate(
                                cxx = {ARROW_FRAGMENT + "<int64_t>", "int64_t"},
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                    "Long"
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.context.ffi.FFIVertexDataContext",
                    templates = {
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,int64_t>",
                                    "int64_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ">",
                                    "Long"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,int64_t>",
                                    "double"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ">",
                                    "Double"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,int64_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,int32_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + INTEGER
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t,double>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + DOUBLE
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ">",
                                    "Integer"
                                }),
                        // vd int32_t
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int32_t,int64_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + INTEGER
                                            + ","
                                            + LONG
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int32_t,int32_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + INTEGER
                                            + ","
                                            + INTEGER
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,int32_t,double>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + INTEGER
                                            + ","
                                            + DOUBLE
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                            + "<int64_t,uint64_t,int32_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + INTEGER
                                            + ">",
                                    "Integer"
                                }),
                        // vd double
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,double,int64_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + DOUBLE
                                            + ","
                                            + LONG
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,double,int64_t>",
                                    "int64_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + DOUBLE
                                            + ","
                                            + LONG
                                            + ">",
                                    "Long"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,double,int32_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + DOUBLE
                                            + ","
                                            + INTEGER
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,double,double>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + DOUBLE
                                            + ","
                                            + DOUBLE
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                            + "<int64_t,uint64_t,double>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_STRING_ED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + DOUBLE
                                            + ","
                                            + ">",
                                    "Integer"
                                }),
                        // vd std string
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                            + "<int64_t,uint64_t,int64_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + LONG
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                            + "<int64_t,uint64_t,int32_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + INTEGER
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                            + "<int64_t,uint64_t,double>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_STRING_VD_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ","
                                            + DOUBLE
                                            + ">",
                                    "Integer"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    CPP_ARROW_PROJECTED_STRING_VED_FRAGMENT + "<int64_t,uint64_t>",
                                    "int32_t"
                                },
                                java = {
                                    JAVA_ARROW_PROJECTED_STRING_VED_FRAGMENT
                                            + "<"
                                            + LONG
                                            + ","
                                            + LONG
                                            + ">",
                                    "Integer"
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.DefaultMessageManager",
                    functionTemplates = {
                        @FFIFunGen(
                                name = "sendToFragment",
                                returnType = "void",
                                parameterTypes = {"int", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {"std::vector<char>"},
                                            java = {"com.alibaba.graphscope.stdcxx.FFIByteVector"})
                                }),
                        @FFIFunGen(
                                name = "getPureMessage",
                                returnType = "boolean",
                                parameterTypes = {"MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {"std::vector<char>"},
                                            java = {"com.alibaba.graphscope.stdcxx.FFIByteVector"})
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.ParallelMessageManager",
                    functionTemplates = {
                        @FFIFunGen(
                                name = "sendToFragment",
                                returnType = "void",
                                parameterTypes = {"int", "MSG_T", "int"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {"std::vector<char>"},
                                            java = {"com.alibaba.graphscope.stdcxx.FFIByteVector"})
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughIEdgesArrowProjected",
                                returnType = "void",
                                parameterTypes = {
                                    "FRAG_T",
                                    "com.alibaba.graphscope.ds.Vertex",
                                    "MSG_T",
                                    "int",
                                    "VDATA_T"
                                },
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,double,int64_t>",
                                                "double",
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>",
                                                "java.lang.Double",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,int64_t>",
                                                "double",
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>",
                                                "java.lang.Double",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.LongLongDouble"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,double,int32_t>",
                                                "double",
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Integer>",
                                                "java.lang.Double",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.DoubleIntegerDouble"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughOEdgesArrowProjected",
                                returnType = "void",
                                parameterTypes = {
                                    "FRAG_T",
                                    "com.alibaba.graphscope.ds.Vertex",
                                    "MSG_T",
                                    "int",
                                    "VDATA_T"
                                },
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,int64_t>",
                                                DOUBLE_MSG,
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.LongLongDouble"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,double,int64_t>",
                                                DOUBLE_MSG,
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughEdgesArrowProjected",
                                returnType = "void",
                                parameterTypes = {
                                    "FRAG_T",
                                    "com.alibaba.graphscope.ds.Vertex",
                                    "MSG_T",
                                    "int",
                                    "VDATA_T"
                                },
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,double,int64_t>",
                                                DOUBLE_MSG,
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,int64_t>",
                                                DOUBLE_MSG,
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.LongLongDouble"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "syncStateOnOuterVertexArrowProjected",
                                returnType = "void",
                                parameterTypes = {
                                    "FRAG_T",
                                    "com.alibaba.graphscope.ds.Vertex",
                                    "MSG_T",
                                    "int",
                                    "VDATA_T"
                                },
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,double,int64_t>",
                                                DOUBLE_MSG,
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongDouble"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,int64_t>",
                                                LONG_MSG,
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.LongLongLong"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "syncStateOnOuterVertexArrowProjectedNoMsg",
                                returnType = "void",
                                parameterTypes = {
                                    "FRAG_T",
                                    "com.alibaba.graphscope.ds.Vertex",
                                    "int",
                                    "VDATA_T"
                                },
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,double,int64_t>",
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLong"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,int64_t>",
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.LongLong"
                                            }),
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.communication.FFICommunicator",
                    functionTemplates = {
                        @FFIFunGen(
                                name = "sum",
                                returnType = "void",
                                parameterTypes = {"MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = DOUBLE_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.DoubleMsg"),
                                    @CXXTemplate(
                                            cxx = LONG_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.LongMsg")
                                }),
                        @FFIFunGen(
                                name = "min",
                                returnType = "void",
                                parameterTypes = {"MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = DOUBLE_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.DoubleMsg"),
                                    @CXXTemplate(
                                            cxx = LONG_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.LongMsg")
                                }),
                        @FFIFunGen(
                                name = "max",
                                returnType = "void",
                                parameterTypes = {"MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = DOUBLE_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.DoubleMsg"),
                                    @CXXTemplate(
                                            cxx = LONG_MSG,
                                            java =
                                                    "com.alibaba.graphscope.parallel.message.LongMsg")
                                }),
                        @FFIFunGen(
                                name = "sendTo",
                                returnType = "void",
                                parameterTypes = {"int", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = "std::vector<char>",
                                            java = "com.alibaba.graphscope.stdcxx.FFIByteVector")
                                }),
                        @FFIFunGen(
                                name = "receiveFrom",
                                returnType = "void",
                                parameterTypes = {"int", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = "std::vector<char>",
                                            java = "com.alibaba.graphscope.stdcxx.FFIByteVector")
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.MessageInBuffer",
                    functionTemplates = {
                        @FFIFunGen(
                                name = "getPureMessage",
                                parameterTypes = {
                                    "MSG_T",
                                },
                                returnType = "boolean",
                                templates = {
                                    @CXXTemplate(
                                            cxx = {"std::vector<char>"},
                                            java = {"com.alibaba.graphscope.stdcxx.FFIByteVector"}),
                                }),
                        @FFIFunGen(
                                name = "getMessageArrowProjected",
                                parameterTypes = {
                                    "FRAG_T",
                                    "com.alibaba.graphscope.ds.Vertex",
                                    "MSG_T",
                                    "VDATA_T"
                                },
                                returnType = "boolean",
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,double,int64_t>",
                                                "grape::EmptyType",
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Long>",
                                                "com.alibaba.graphscope.ds.EmptyType",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.DoubleLongEmpty"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,int64_t,int64_t>",
                                                LONG_MSG,
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Long,java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.LongLongLong"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CPP_ARROW_PROJECTED_FRAGMENT
                                                        + "<int64_t,uint64_t,double,int32_t>",
                                                DOUBLE_MSG,
                                                "any"
                                            },
                                            java = {
                                                JAVA_ARROW_PROJECTED_FRAGMENT
                                                        + "<java.lang.Long,java.lang.Long,java.lang.Double,java.lang.Integer>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg",
                                                "com.alibaba.graphscope.runtime.UnusedImpl.DoubleIntegerDouble"
                                            }),
                                })
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.ParallelPropertyMessageManager",
                    functionTemplates = {
                        @FFIFunGen(
                                name = "syncStateOnOuterVertex",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                GS_PRIMITIVE_MESSAGE + "<int64_t>"
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.PrimitiveMessage<java.lang.Long>"
                                            })
                                }),
                        @FFIFunGen(
                                name = "syncStateOnOuterVertexNoMsg",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "OID"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                "int64_t"
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "Long"
                                            })
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughOEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                GS_PRIMITIVE_MESSAGE + "<int64_t>"
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.PrimitiveMessage<java.lang.Long>"
                                            })
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughIEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                GS_PRIMITIVE_MESSAGE + "<int64_t>"
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.PrimitiveMessage<java.lang.Long>"
                                            })
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                GS_PRIMITIVE_MESSAGE + "<int64_t>"
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.PrimitiveMessage<java.lang.Long>"
                                            })
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.parallel.PropertyMessageManager",
                    functionTemplates = {
                        @FFIFunGen(
                                name = "syncStateOnOuterVertex",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughIEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughOEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "sendMsgThroughEdges",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                }),
                        @FFIFunGen(
                                name = "getMessage",
                                returnType = "void",
                                parameterTypes = {"FRAG_T", "MSG_T"},
                                templates = {
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                DOUBLE_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.DoubleMsg"
                                            }),
                                    @CXXTemplate(
                                            cxx = {
                                                CppClassName.ARROW_FRAGMENT + "<int64_t>",
                                                LONG_MSG
                                            },
                                            java = {
                                                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                                                "com.alibaba.graphscope.parallel.message.LongMsg"
                                            }),
                                }),
                    })
        })
public class AnnotationInvoker {}

package com.alibaba.graphscope.annotation.sample;

import static com.alibaba.graphscope.utils.CppClassName.ARROW_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_EMPTY_TYPE;
import static com.alibaba.graphscope.utils.CppClassName.GS_VERTEX_ARRAY;

import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIGenBatch;

@FFIGenBatch(
        value = {
            @FFIGen(type = "com.alibaba.graphscope.parallel.message.DoubleMsg"),
            @FFIGen(type = "com.alibaba.graphscope.parallel.message.LongMsg"),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.Vertex",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.VertexRange",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer")
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.VertexArray",
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t"},
                                java = {"Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"int32_t", "uint64_t"},
                                java = {"Integer", "Long"}),
                        @CXXTemplate(
                                cxx = {"double", "uint64_t"},
                                java = {"Double", "Long"}),
                        @CXXTemplate(
                                cxx = {"uint64_t", "uint64_t"},
                                java = {"Long", "Long"}),
                        @CXXTemplate(
                                cxx = {"uint32_t", "uint64_t"},
                                java = {"Integer", "Long"}),
                        @CXXTemplate(
                                cxx = {"double", "uint64_t"},
                                java = {"Double", "Long"}),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.Nbr",
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
                    type = "com.alibaba.graphscope.ds.AdjList",
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
                    type = "com.alibaba.graphscope.ds.GSVertexArray",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "double", java = "Double"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "double", java = "Double"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.stdcxx.StdVector",
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "double", java = "Double"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                        @CXXTemplate(
                                cxx = GS_VERTEX_ARRAY + "<double>",
                                java = "com.alibaba.graphscope.ds.GSVertexArray" + "<Double>"),
                        @CXXTemplate(
                                cxx = GS_VERTEX_ARRAY + "<int32_t>",
                                java = "com.alibaba.graphscope.ds.GSVertexArray" + "<Integer>"),
                        @CXXTemplate(
                                cxx = GS_VERTEX_ARRAY + "<int64_t>",
                                java = "com.alibaba.graphscope.ds.GSVertexArray" + "<Long>"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.PropertyNbr",
                    templates = {
                        @CXXTemplate(cxx = "uint64_t", java = "Long"),
                        @CXXTemplate(cxx = "uint32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.ds.PropertyNbrUnit",
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
                    templates = {
                        @CXXTemplate(cxx = "int64_t", java = "Long"),
                        @CXXTemplate(cxx = "int32_t", java = "Integer"),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.fragment.ArrowProjectedFragment",
                    templates = {
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", GRAPE_EMPTY_TYPE, "int64_t"},
                                java = {
                                    "Long",
                                    "Long",
                                    "com.alibaba.graphscope.ds.EmptyType",
                                    "Long"
                                }),
                        @CXXTemplate(
                                cxx = {"int64_t", "uint64_t", "double", "int64_t"},
                                java = {"Long", "Long", "Double", "Long"})
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
                    type = "com.alibaba.graphscope.ds.ProjectedAdjList",
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
            @FFIGen(type = "com.alibaba.graphscope.column.IColumn"),
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
                                    ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,grape::EmptyType,int64_t>"
                                },
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,com.alibaba.graphscope.ds.EmptyType,Long>"
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
                                    ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,grape::EmptyType,int64_t>"
                                },
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,com.alibaba.graphscope.ds.EmptyType,Long>"
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
                                    ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,grape::EmptyType,int64_t>"
                                },
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,com.alibaba.graphscope.ds.EmptyType,Long>"
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.stdcxx.StdSharedPtr",
                    templates = {
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
                                        "gs::DoubleColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.DoubleColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.graphscope.ds.EmptyType,java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        "gs::IntColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.IntColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.graphscope.ds.EmptyType,java.lang.Long>>"),
                        @CXXTemplate(
                                cxx =
                                        "gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
                                java =
                                        "com.alibaba.graphscope.column.LongColumn<com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.graphscope.ds.EmptyType,java.lang.Long>>"),
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
                    type = "com.alibaba.graphscope.context.ffi.FFILabeledVertexPropertyContext",
                    templates = {
                        @CXXTemplate(
                                cxx = {ARROW_FRAGMENT + "<int64_t>"},
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>"
                                })
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.context.ffi.FFIVertexDataContext",
                    templates = {
                        @CXXTemplate(
                                cxx = {
                                    ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,grape::EmptyType,int64_t>",
                                    "double"
                                },
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.graphscope.ds.EmptyType,java.lang.Long>",
                                    "Double"
                                }),
                        @CXXTemplate(
                                cxx = {
                                    ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,grape::EmptyType,int64_t>",
                                    "int64_t"
                                },
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.graphscope.ds.EmptyType,java.lang.Long>",
                                    "Long"
                                }),
                    }),
            @FFIGen(
                    type = "com.alibaba.graphscope.context.ffi.FFIVertexPropertyContext",
                    templates = {
                        @CXXTemplate(
                                cxx = {
                                    ARROW_PROJECTED_FRAGMENT
                                            + "<int64_t,uint64_t,grape::EmptyType,int64_t>"
                                },
                                java = {
                                    "com.alibaba.graphscope.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.graphscope.ds.EmptyType,java.lang.Long>"
                                })
                    }),
            //            @FFIGen(
            //                    type = "com.alibaba.graphscope.parallel.PropertyMessageManager",
            //                    functionTemplates = {
            //                        @FFIFunGen(
            //                                name = "sendMsgThroughIEdges",
            //                                returnType = "void",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                        @FFIFunGen(
            //                                name = "sendMsgThroughOEdges",
            //                                returnType = "void",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                        @FFIFunGen(
            //                                name = "sendMsgThroughEdges",
            //                                returnType = "void",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                        @FFIFunGen(
            //                                name = "syncStateOnOuterVertex",
            //                                returnType = "void",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                        @FFIFunGen(
            //                                name = "getMessage",
            //                                returnType = "boolean",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                    }),
            //            @FFIGen(
            //                    type = "com.alibaba.graphscope.parallel.DefaultMessageManager",
            //                    functionTemplates = {
            //                        @FFIFunGen(
            //                                name = "sendMsgThroughIEdges",
            //                                returnType = "void",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                        @FFIFunGen(
            //                                name = "sendMsgThroughOEdges",
            //                                returnType = "void",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                        @FFIFunGen(
            //                                name = "sendMsgThroughEdges",
            //                                returnType = "void",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                        @FFIFunGen(
            //                                name = "syncStateOnOuterVertex",
            //                                returnType = "void",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                        @FFIFunGen(
            //                                name = "getMessage",
            //                                returnType = "boolean",
            //                                parameterTypes = {"FRAG_T", "MSG_T"},
            //                                templates = {
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                DOUBLE_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.DoubleMsg"
            //                                            }),
            //                                    @CXXTemplate(
            //                                            cxx = {
            //                                                CppClassName.ARROW_FRAGMENT +
            // "<int64_t>",
            //                                                LONG_MSG
            //                                            },
            //                                            java = {
            //
            // "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
            //
            // "com.alibaba.graphscope.parallel.message.LongMsg"
            //                                            })
            //                                }),
            //                    }),
        })
public class AnnotationInvoker {}

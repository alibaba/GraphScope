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

package com.alibaba.graphscope.parallel;

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_DEFAULT_MESSAGE_MANAGER;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_DEFAULT_MESSAGE_MANAGER_H;
import static com.alibaba.graphscope.utils.JNILibraryName.JNI_LIBRARY_NAME;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;

/**
 * The default message manager, used in serial apps {@link DefaultAppBase} and {@link
 * com.alibaba.graphscope.app.ProjectedDefaultAppBase}.
 */
@FFIGen(library = JNI_LIBRARY_NAME)
@FFITypeAlias(GRAPE_DEFAULT_MESSAGE_MANAGER)
@CXXHead({
    GRAPE_ADJ_LIST_H,
    GRAPE_PARALLEL_DEFAULT_MESSAGE_MANAGER_H,
    GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
    ARROW_PROJECTED_FRAGMENT_H,
    CORE_JAVA_JAVA_MESSAGES_H
})
public interface DefaultMessageManager extends MessageManagerBase {

    /**
     * Get the message received for specified vertex during last super step.
     *
     * @param frag bound fragment.
     * @param vertex querying vertex.
     * @param msg msg place hold.
     * @param <FRAG_T> frag type.
     * @param <MSG_T> msg type.
     * @return true if really got a message.
     */
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("GetMessage")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Get the message received for specified vertex during last super step.
     *
     * @param frag bound fragment.
     * @param vertex querying vertex.
     * @param msg msg place hold.
     * @param <FRAG_T> frag type.
     * @param <MSG_T> msg type.
     * @return true if really got a message.
     */
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("GetMessage")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Send a msg to the fragment where the querying outer vertex is an inner vertex.
     *
     * @param frag fragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Send a msg to the fragment where the querying outer vertex is an inner vertex.
     *
     * @param frag fragment.
     * @param vertex querying vertex.
     * @param msg msg to send.
     * @param <FRAG_T> fragment type.
     * @param <MSG_T> message type.
     */
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    /**
     * Send the msg to
     *
     * @param frag
     * @param vertex
     * @param msg
     * @param <FRAG_T>
     * @param <MSG_T>
     */
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
    //            java = {
    //                "com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>",
    // DOUBLE_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.DoubleMsg"
    //            })
    // @CXXTemplate(
    //            cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
    //            java = {
    //
    // "com.alibaba.graphscope.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
    //                "com.alibaba.grape.parallel.message.LongMsg"
    //            })
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);
}

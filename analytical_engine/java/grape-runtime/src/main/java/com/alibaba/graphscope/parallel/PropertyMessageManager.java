package com.alibaba.graphscope.parallel;

import static com.alibaba.graphscope.utils.CppClassName.DOUBLE_MSG;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.graphscope.utils.CppClassName.LONG_MSG;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead({
    CppHeaderName.PROPERTY_MESSAGE_MANAGER_H,
    CppHeaderName.ARROW_FRAGMENT_H,
    CORE_JAVA_JAVA_MESSAGES_H,
    CppHeaderName.CORE_JAVA_TYPE_ALIAS_H
})
@CXXHead("cstdint")
@FFITypeAlias(CppClassName.PROPERTY_MESSAGE_MANAGER)
public interface PropertyMessageManager extends MessageManagerBase {

    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.DoubleMsg"
            })
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.LongMsg"
            })
    @FFINameAlias("SendMsgThroughIEdges")
    <FRAG_T, MSG_T> void sendMsgThroughIEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int eLabelId,
            @CXXReference MSG_T msg);

    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.DoubleMsg"
            })
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.LongMsg"
            })
    @FFINameAlias("SendMsgThroughOEdges")
    <FRAG_T, MSG_T> void sendMsgThroughOEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int eLabelId,
            @CXXReference MSG_T msg);

    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.DoubleMsg"
            })
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.LongMsg"
            })
    @FFINameAlias("SendMsgThroughEdges")
    <FRAG_T, MSG_T> void sendMsgThroughEdges(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            int eLabelId,
            @CXXReference MSG_T msg);

    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.DoubleMsg"
            })
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.LongMsg"
            })
    @FFINameAlias("SyncStateOnOuterVertex")
    <FRAG_T, MSG_T> void syncStateOnOuterVertex(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);

    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.DoubleMsg"
            })
    @CXXTemplate(
            cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {
                "com.alibaba.graphscope.fragment.ArrowFragment<java.lang.Long>",
                "com.alibaba.graphscope.parallel.message.LongMsg"
            })
    @FFINameAlias("GetMessage")
    <FRAG_T, MSG_T> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg);
}

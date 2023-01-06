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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.graphscope.utils.CppClassName.GRAPE_MESSAGE_IN_BUFFER;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;

@FFIGen
@FFITypeAlias(GRAPE_MESSAGE_IN_BUFFER)
@CXXHead({
    GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H,
    GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
    ARROW_PROJECTED_FRAGMENT_H,
    CORE_JAVA_TYPE_ALIAS_H,
    CORE_JAVA_JAVA_MESSAGES_H
})
public interface MessageInBuffer extends FFIPointer {
    default <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T, @FFISkip UNUSED_T> boolean getMessage(
            @CXXReference IFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            @CXXReference Vertex<VID_T> vertex,
            @CXXReference MSG_T msg) {
        if (frag.fragmentType().equals(FragmentType.ArrowProjectedFragment)) {
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> arrowProjectedAdaptor =
                    ((ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag);
            return getMessageArrowProjected(
                    arrowProjectedAdaptor.getArrowProjectedFragment(),
                    vertex,
                    msg,
                    MessageUtils.getUnused(arrowProjectedAdaptor, msg.getClass()));
        } else if (frag.fragmentType().equals(FragmentType.ImmutableEdgecutFragment)) {
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> immutableAdaptor =
                    (ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T>) frag;
            return getMessageImmutable(
                    immutableAdaptor.getImmutableFragment(),
                    vertex,
                    msg,
                    MessageUtils.getUnused(immutableAdaptor, msg.getClass()));
        }
        return false;
    }

    @FFINameAlias("GetMessage")
    <FRAG_T extends ArrowFragment, MSG_T, @FFISkip UNUSED> boolean getMessage(
            @CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex,
            @CXXReference MSG_T msg,
            @FFISkip UNUSED unused);

    @FFINameAlias("GetMessage")
    <
                    @FFISkip OID_T,
                    @FFISkip VID_T,
                    @FFISkip VDATA_T,
                    @FFISkip EDATA_T,
                    FRAG_T extends ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>,
                    MSG_T,
                    @FFISkip UNUSED_T>
            boolean getMessageArrowProjected(
                    @CXXReference FRAG_T frag,
                    @CXXReference Vertex<VID_T> vertex,
                    @CXXReference MSG_T msg,
                    @FFISkip UNUSED_T unused);

    @FFINameAlias("GetMessage")
    <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T, @FFISkip UNUSED_T> boolean getMessageImmutable(
            @CXXReference ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
            @CXXReference Vertex<VID_T> vertex,
            @CXXReference MSG_T msg,
            @FFISkip UNUSED_T unused);

    /**
     * Get message into target MSG_T.
     *
     * @param msg received msg.
     * @return
     */
    @FFINameAlias("GetMessage")
    <MSG_T> boolean getPureMessage(@CXXReference MSG_T msg);

    @FFIFactory
    interface Factory {
        MessageInBuffer create();
    }
}

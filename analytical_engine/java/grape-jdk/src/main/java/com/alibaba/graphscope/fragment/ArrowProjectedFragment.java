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

package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.CPP_ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import com.alibaba.graphscope.ds.BaseTypedArray;
import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.ProjectedNbr;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.impl.ProjectedAdjListImpl;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
// import com.alibaba.graphscope.utils.LongIdParser;

/**
 * Java wrapper for <a href=
 * "https://github.com/alibaba/GraphScope/blob/main/analytical_engine/core/fragment/arrow_projected_fragment.h#L338">ArrowProjectedFragment</a>
 *
 * @param <OID_T> original id type
 * @param <VID_T> vertex id type
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 */
@FFIGen
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CPP_ARROW_PROJECTED_FRAGMENT)
public interface ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>
        extends BaseArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> {

    // private LongIdParser idParser = new LongIdParser(fragment.fnum(), 1);;

    default ProjectedAdjList<VID_T, EDATA_T> getIncomingAdjList(Vertex<VID_T> vertex, Class<? extends VID_T> vidType, Class<? extends EDATA_T> edataType) {
        PropertyNbrUnit<VID_T> nbrUnit = getInEdgesPtr();
        long nbrUnitInitAddress = nbrUnit.getAddress();
        long offsetEndPtrFirstAddr = getIEOffsetsEndPtr();
        long offsetBeginPtrFirstAddr = getIEOffsetsBeginPtr();
        // long offset = idParser.getOffset(lid);
        long offset = (long) vertex.getValue();
        long oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + offset * 8);
        long oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + offset * 8);
        long curAddress = nbrUnitInitAddress + 16 * oeBeginOffset;
        long endAddress = nbrUnitInitAddress + 16 * oeEndOffset;
        ProjectedNbr<VID_T, EDATA_T> begin = FFITypeFactoryhelper.newProjectedNbr(vidType, edataType);
        begin.setAddress(curAddress);
        ProjectedNbr<VID_T, EDATA_T> end = FFITypeFactoryhelper.newProjectedNbr(vidType, edataType);
        end.setAddress(endAddress);
        return new ProjectedAdjListImpl<VID_T, EDATA_T>(begin, end);
    }

    default ProjectedAdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex, Class<? extends VID_T> vidType, Class<? extends EDATA_T> edataType) {
        PropertyNbrUnit<VID_T> nbrUnit = getOutEdgesPtr();
        long nbrUnitInitAddress = nbrUnit.getAddress();
        long offsetBeginPtrFirstAddr = getOEOffsetsBeginPtr();
        long offsetEndPtrFirstAddr = getOEOffsetsEndPtr();
        // long offset = idParser.getOffset(lid);
        long offset = (long) vertex.getValue();
        long oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + offset * 8);
        long oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + offset * 8);
        long curAddress = nbrUnitInitAddress + 16 * oeBeginOffset;
        long endAddress = nbrUnitInitAddress + 16 * oeEndOffset;
        ProjectedNbr<VID_T, EDATA_T> begin = FFITypeFactoryhelper.newProjectedNbr(vidType, edataType);
        begin.setAddress(curAddress);
        ProjectedNbr<VID_T, EDATA_T> end = FFITypeFactoryhelper.newProjectedNbr(vidType, edataType);
        end.setAddress(endAddress);
        return new ProjectedAdjListImpl<VID_T, EDATA_T>(begin, end);
    }

    @FFINameAlias("get_edata_array_accessor")
    @CXXReference
    BaseTypedArray<EDATA_T> getEdataArrayAccessor();

    @FFINameAlias("get_vdata_array_accessor")
    @CXXReference
    BaseTypedArray<VDATA_T> getVdataArrayAccessor();
}

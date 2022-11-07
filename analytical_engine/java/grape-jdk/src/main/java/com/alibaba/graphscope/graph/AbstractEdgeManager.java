/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.graph;

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import com.alibaba.graphscope.ds.PrimitiveTypedArray;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.LongPointerAccessor;
import com.alibaba.graphscope.utils.array.PrimitiveArray;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Defines common behavior for abstract edge manager. Notice about the type
 * parameters, since giraph rely on writable.
 */
public abstract class AbstractEdgeManager<VID_T, GRAPE_OID_T, BIZ_OID_T, GRAPE_ED_T, BIZ_EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(AbstractEdgeManager.class.getName());

    private Vertex<VID_T> grapeVertex;
    private ArrowProjectedFragment<GRAPE_OID_T, VID_T, ?, GRAPE_ED_T> fragment;
    private PropertyNbrUnit<VID_T> nbrUnit;
    private VertexIdManager<VID_T, BIZ_OID_T> vertexIdManager;

    private LongPointerAccessor oeOffsetsBeginAccessor, oeOffsetsEndAccessor;
    private long nbrUnitEleSize, nbrUnitInitAddress;
    public CSRHolder csrHolder;
    protected TupleIterable edgeIterable;
    protected List<TupleIterable> edgeIterables;
    private int VID_SHIFT_BITS, VID_SIZE_IN_BYTE;
    private List<GRAPE_OID_T> oids;
    private long innerVerticesNum;
    private int vid_t, edata_t; // 0 for long ,1 for int
    private Class<? extends GRAPE_ED_T> edataClass;
    private Class<? extends BIZ_EDATA_T> bizEdataClass;
    private Class<? extends VID_T> vidClass;
    private Class<? extends BIZ_OID_T> bizOidClass;

    public void init(
            IFragment<GRAPE_OID_T, VID_T, ?, GRAPE_ED_T> fragment,
            VertexIdManager<VID_T, BIZ_OID_T> vertexIdManager,
            Class<? extends BIZ_OID_T> bizOidClass,
            Class<? extends VID_T> vidClass,
            Class<? extends GRAPE_ED_T> grapeEdataClass,
            Class<? extends BIZ_EDATA_T> bizEdataClass,
            BiConsumer<FFIByteVectorInputStream, PrimitiveArray<BIZ_EDATA_T>> consumer) {
        this.edataClass = grapeEdataClass;
        this.bizEdataClass = bizEdataClass;
        this.bizOidClass = bizOidClass;
        this.fragment =
                ((ArrowProjectedAdaptor<GRAPE_OID_T, VID_T, ?, GRAPE_ED_T>) fragment)
                        .getArrowProjectedFragment();
        this.vertexIdManager = vertexIdManager;

        initFields();
        if (vidClass.equals(Long.class)) {
            VID_SHIFT_BITS = 3; // shift 3 bits <--> * 8
            VID_SIZE_IN_BYTE = 8; // long = 8bytes
            vid_t = 0;
        } else if (vidClass.equals(Integer.class)) {
            VID_SHIFT_BITS = 2;
            VID_SIZE_IN_BYTE = 4;
            vid_t = 1;
        }
        this.vidClass = vidClass;
        edata_t = grapeEdata2Int();
        PrimitiveTypedArray<GRAPE_ED_T> newTypedArray =
                FFITypeFactoryhelper.newPrimitiveTypedArray(edataClass);
        newTypedArray.setAddress(this.fragment.getEdataArrayAccessor().getAddress());
        csrHolder = new CSRHolder(newTypedArray, consumer);
        edgeIterable = new TupleIterable(csrHolder);
        edgeIterables = null;
    }

    public void init(
            IFragment<GRAPE_OID_T, VID_T, ?, GRAPE_ED_T> fragment,
            VertexIdManager<VID_T, BIZ_OID_T> vertexIdManager,
            Class<? extends BIZ_OID_T> bizOidClass,
            Class<? extends VID_T> vidClass,
            Class<? extends GRAPE_ED_T> grapeEdataClass,
            Class<? extends BIZ_EDATA_T> bizEdataClass,
            BiConsumer<FFIByteVectorInputStream, PrimitiveArray<BIZ_EDATA_T>> consumer,
            int numCores) {
        init(
                fragment,
                vertexIdManager,
                bizOidClass,
                vidClass,
                grapeEdataClass,
                bizEdataClass,
                consumer);
        edgeIterables = Lists.newArrayListWithCapacity(numCores);
        for (int i = 0; i < numCores; ++i) {
            edgeIterables.add(new TupleIterable(csrHolder));
        }
    }

    public int getNumEdgesImpl(long lid) {
        long oeBeginOffset = oeOffsetsBeginAccessor.get(lid);
        long oeEndOffset = oeOffsetsEndAccessor.get(lid);
        return (int) (oeEndOffset - oeBeginOffset);
    }

    private void initFields() {
        nbrUnit = this.fragment.getOutEdgesPtr();
        nbrUnitEleSize = nbrUnit.elementSize();
        nbrUnitInitAddress = nbrUnit.getAddress();
        innerVerticesNum = this.fragment.getInnerVerticesNum();
        oeOffsetsBeginAccessor = new LongPointerAccessor(this.fragment.getOEOffsetsBeginPtr());
        oeOffsetsEndAccessor = new LongPointerAccessor(this.fragment.getOEOffsetsEndPtr());
        logger.info(
                "nbr unit element size: {}, init address {}",
                nbrUnit.elementSize(),
                nbrUnit.getAddress());
    }

    public class TupleIterator implements Iterator<GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T>> {
        private GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T> grapeEdge = new GrapeEdge<>();
        private int nbrPos;
        private long numEdge;
        private PrimitiveArray<BIZ_OID_T> dstOids;
        private PrimitiveArray<VID_T> dstLids;
        private PrimitiveArray<BIZ_EDATA_T> edatas;
        private int[] nbrPositions;
        private long[] numOfEdges;

        public TupleIterator(
                long[] numOfEdges,
                int[] nbrPositions,
                PrimitiveArray<BIZ_OID_T> dstOids,
                PrimitiveArray<VID_T> dstLids,
                PrimitiveArray<BIZ_EDATA_T> edatas) {
            this.numOfEdges = numOfEdges;
            this.nbrPositions = nbrPositions;
            this.dstOids = dstOids;
            this.dstLids = dstLids;
            this.edatas = edatas;
        }

        public void setLid(int lid) {
            numEdge = numOfEdges[lid];
            nbrPos = nbrPositions[lid];
        }

        @Override
        public boolean hasNext() {
            return numEdge > 0;
        }

        @Override
        public GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T> next() {
            grapeEdge.dstLid = dstLids.get(nbrPos);
            grapeEdge.dstOid = dstOids.get(nbrPos);
            grapeEdge.value = edatas.get(nbrPos++);
            numEdge -= 1;
            return grapeEdge;
        }
    }

    public class CSRHolder {

        private long totalNumOfEdges;

        public long[] nbrUnitAddrs, numOfEdges;
        public PrimitiveArray<BIZ_OID_T> dstOids;
        public PrimitiveArray<VID_T> dstLids;
        public PrimitiveArray<BIZ_EDATA_T> edatas;
        public int[] nbrPositions;
        private BiConsumer<FFIByteVectorInputStream, PrimitiveArray<BIZ_EDATA_T>> consumer;

        public CSRHolder(
                PrimitiveTypedArray<GRAPE_ED_T> edataArray,
                BiConsumer<FFIByteVectorInputStream, PrimitiveArray<BIZ_EDATA_T>> consumer) {
            this.consumer = consumer;
            totalNumOfEdges = getTotalNumOfEdges();
            nbrUnitAddrs = new long[(int) innerVerticesNum];
            numOfEdges = new long[(int) innerVerticesNum];
            nbrPositions = new int[(int) innerVerticesNum];
            // marks the mapping between lid to start pos of nbr, i.e. offset.
            // the reason why we don't resuse oeBegin Offset is that eid may not sequential.
            //            edatas = (BIZ_EDATA_T[]) Array.newInstance(bizEdataClass,
            // (int)totalNumOfEdges);
            //            edatas = (BIZ_EDATA_T[]) new Object[(int) totalNumOfEdges];
            //            dstOids = (BIZ_OID_T[]) Array.newInstance(bizOidClass, (int)
            // totalNumOfEdges);
            //            dstLids = (VID_T[]) Array.newInstance(vidClass, (int) totalNumOfEdges);
            //            dstLids = (VID_T[]) new Object[(int) totalNumOfEdges];
            edatas = PrimitiveArray.create(bizEdataClass, (int) totalNumOfEdges);
            dstOids = PrimitiveArray.create(bizOidClass, (int) totalNumOfEdges);
            dstLids = PrimitiveArray.create(vidClass, (int) totalNumOfEdges);
            try {
                initArrays(edataArray);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private long getTotalNumOfEdges() {
            long largest = oeOffsetsEndAccessor.get(innerVerticesNum - 1);
            //            long smallest = JavaRuntime.getLong(offsetBeginPtrFirstAddr + (0 <<
            // VID_SHIFT_BITS));
            long smallest = oeOffsetsBeginAccessor.get(0);
            return largest - smallest;
        }

        private void initArrays(PrimitiveTypedArray<GRAPE_ED_T> edataArray) throws IOException {
            int tmpSum = 0;
            long oeBeginOffset, oeEndOffset;
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
                oeBeginOffset = oeOffsetsBeginAccessor.get(lid);
                oeEndOffset = oeOffsetsEndAccessor.get(lid);
                nbrUnitAddrs[(int) lid] = nbrUnitInitAddress + (oeBeginOffset * nbrUnitEleSize);
                numOfEdges[(int) lid] = oeEndOffset - oeBeginOffset;
                tmpSum += numOfEdges[(int) lid];
            }
            if (tmpSum != totalNumOfEdges) {
                throw new IllegalStateException("not equal: " + tmpSum + ", " + totalNumOfEdges);
            }

            // deserialize back from csr.
            int index = 0;
            for (int lid = 0; lid < innerVerticesNum; ++lid) {
                long curAddrr = nbrUnitAddrs[lid];
                nbrPositions[lid] = index;
                for (int j = 0; j < numOfEdges[lid]; ++j) {
                    if (vid_t == 0) {
                        Long dstLid = JavaRuntime.getLong(curAddrr);
                        dstLids.set(index, (VID_T) dstLid);
                        dstOids.set(index++, vertexIdManager.lid2Oid((VID_T) dstLid));
                    } else {
                        Integer dstLid = JavaRuntime.getInt(curAddrr);
                        dstLids.set(index, (VID_T) dstLid);
                        dstOids.set(index++, vertexIdManager.lid2Oid((VID_T) dstLid));
                    }
                    curAddrr += nbrUnitEleSize;
                }
            }
            // fill in edata arrays.
            fillInEdataArray(edataArray);
        }

        private void fillInEdataArray(PrimitiveTypedArray<GRAPE_ED_T> edataArray)
                throws IOException {
            // first try to set directly.
            int index = 0;
            if (bizEdataClass.equals(edataClass)) {
                logger.info("biz edata {} == grape edata, try to read direct", edata_t);
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid] + VID_SIZE_IN_BYTE;
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = JavaRuntime.getLong(curAddrr);
                        edatas.set(index++, (BIZ_EDATA_T) edataArray.get(eid));
                        curAddrr += nbrUnitEleSize;
                    }
                }
            } else {
                logger.warn("Trying to load via serialization and deserialization");
                FFIByteVectorInputStream inputStream =
                        new FFIByteVectorInputStream(
                                generateEdataString(nbrUnitAddrs, numOfEdges, edataArray));

                rebuildEdatasFromStream(inputStream, edatas);

                logger.info("Finish creating edata array");
                inputStream.getVector().delete();
            }
            if (logger.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (int i = 0; i < edatas.size(); ++i) {
                    sb.append(edatas.get(i) + ",");
                }
                sb.append("]");
                logger.info("edata array:" + sb.toString());
            }
        }

        // By default, we check whether BIZ_EDATA_T is the same as GRAPE_EDATA_T, if so, we just
        // convert and set.
        // If not we turn for this function.
        public void rebuildEdatasFromStream(
                FFIByteVectorInputStream inputStream, PrimitiveArray<BIZ_EDATA_T> edatas) {
            if (consumer == null) {
                throw new IllegalStateException(
                        "You should override rebuildEdatasFromStream, since automatic conversion"
                                + " failed.");
            }
            consumer.accept(inputStream, edatas);
        }
    }

    public class TupleIterable implements Iterable<GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T>> {

        private TupleIterator iterator;
        private CSRHolder csrHolder;

        public TupleIterable(CSRHolder csrHolder) {
            this.csrHolder = csrHolder;
            iterator =
                    new TupleIterator(
                            csrHolder.numOfEdges,
                            csrHolder.nbrPositions,
                            csrHolder.dstOids,
                            csrHolder.dstLids,
                            csrHolder.edatas);
        }

        public void setLid(long lid) {
            this.iterator.setLid((int) lid);
        }

        /**
         * Returns an iterator over elements of type {@code T}.
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<GrapeEdge<VID_T, BIZ_OID_T, BIZ_EDATA_T>> iterator() {
            return iterator;
        }
    }

    private int grapeEdata2Int() {
        if (edataClass.equals(Long.class) || edataClass.equals(long.class)) {
            logger.info("edata: Long");
            return 0;
        } else if (edataClass.equals(Integer.class) || edataClass.equals(int.class)) {
            logger.info("edata: Int");
            return 1;
        } else if (edataClass.equals(Double.class) || edataClass.equals(double.class)) {
            logger.info("edata: Double");
            return 2;
        } else if (edataClass.equals(Float.class) || edataClass.equals(float.class)) {
            logger.info("edata: Float");
            return 3;
        } else if (edataClass.equals(String.class)) {
            logger.info("edata: String");
            return 4;
        }
        throw new IllegalStateException("Cannot recognize edata type " + edataClass);
    }

    private FFIByteVector generateEdataString(
            long[] nbrUnitAddrs, long[] numOfEdges, PrimitiveTypedArray<GRAPE_ED_T> edataArray)
            throws IOException {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        switch (edata_t) {
            case 0:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = JavaRuntime.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        Long longValue = (Long) edata;
                        outputStream.writeLong(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            case 1:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = JavaRuntime.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        Integer longValue = (Integer) edata;
                        outputStream.writeInt(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            case 2:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = JavaRuntime.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        Double longValue = (Double) edata;
                        outputStream.writeDouble(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            case 3:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = JavaRuntime.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        Float longValue = (Float) edata;
                        outputStream.writeFloat(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            case 4:
                for (int lid = 0; lid < innerVerticesNum; ++lid) {
                    long curAddrr = nbrUnitAddrs[lid];
                    for (int j = 0; j < numOfEdges[lid]; ++j) {
                        long eid = JavaRuntime.getLong(curAddrr + VID_SIZE_IN_BYTE);
                        GRAPE_ED_T edata = edataArray.get(eid);
                        String longValue = (String) edata;
                        outputStream.writeBytes(longValue);
                        curAddrr += nbrUnitEleSize;
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unexpected edata type: " + edata_t);
        }
        outputStream.finishSetting();
        logger.info("Finish creating stream");
        return outputStream.getVector();
    }
}

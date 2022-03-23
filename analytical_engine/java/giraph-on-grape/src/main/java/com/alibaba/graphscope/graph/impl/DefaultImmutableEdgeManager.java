/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.graph.impl;

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.TypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.graph.EdgeManager;
import com.alibaba.graphscope.graph.VertexIdManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.google.common.collect.Lists;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Defeault edge manager, which extract all edata and oids from grape before accessing them in
 * queries.
 *
 * @param <GRAPE_OID_T>
 * @param <GRAPE_VID_T>
 * @param <GRAPE_VDATA_T>
 * @param <GRAPE_EDATA_T>
 * @param <GIRAPH_OID_T>
 * @param <GIRAPH_EDATA_T>
 */
public class DefaultImmutableEdgeManager<
                GRAPE_OID_T,
                GRAPE_VID_T,
                GRAPE_VDATA_T,
                GRAPE_EDATA_T,
                GIRAPH_OID_T extends WritableComparable,
                GIRAPH_EDATA_T extends Writable>
        implements EdgeManager<GIRAPH_OID_T, GIRAPH_EDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(DefaultImmutableEdgeManager.class);

    private ImmutableClassesGiraphConfiguration<? super GIRAPH_OID_T, ?, ? super GIRAPH_EDATA_T>
            conf;
    private VertexIdManager<GRAPE_VID_T, GIRAPH_OID_T> vertexIdManager;
    private Vertex<GRAPE_VID_T> grapeVertex;
    private ArrowProjectedFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private PropertyNbrUnit<GRAPE_VID_T> nbrUnit;
    private long offsetBeginPtrFirstAddr;
    private long offsetEndPtrFirstAddr;

    private long nbrUnitEleSize;
    private long nbrUnitInitAddress;
    private GenericEdgeIterable edgeIterable;
    private int VID_SHIFT_BITS, VID_SIZE_IN_BYTE;
    private List<GRAPE_OID_T> oids;
    private long innerVerticesNum;
    private int vid_t; // 0 for long ,1 for int

    public DefaultImmutableEdgeManager(
            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            VertexIdManager<GRAPE_VID_T, ? extends GIRAPH_OID_T> idManager,
            ImmutableClassesGiraphConfiguration<?, ?, ?> configuration) {
        this.fragment =
                ((ArrowProjectedAdaptor<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>)
                                fragment)
                        .getArrowProjectedFragment();
        grapeVertex =
                (Vertex<GRAPE_VID_T>)
                        FFITypeFactoryhelper.newVertex(configuration.getGrapeVidClass());
        vertexIdManager = (VertexIdManager<GRAPE_VID_T, GIRAPH_OID_T>) idManager;
        this.conf =
                (ImmutableClassesGiraphConfiguration<
                                ? super GIRAPH_OID_T, ?, ? super GIRAPH_EDATA_T>)
                        configuration;

        if (conf.getGrapeVidClass().equals(Long.class)) {
            VID_SHIFT_BITS = 3; // shift 3 bits <--> * 8
            VID_SIZE_IN_BYTE = 8; // long = 8bytes
            vid_t = 0;
        } else if (conf.getGrapeVidClass().equals(Integer.class)) {
            VID_SHIFT_BITS = 2;
            VID_SIZE_IN_BYTE = 4;
            vid_t = 1;
        }

        // Copy all edges to java memory has too much cost, don't copy
        nbrUnit = this.fragment.getOutEdgesPtr();
        offsetEndPtrFirstAddr = this.fragment.getOEOffsetsEndPtr();
        offsetBeginPtrFirstAddr = this.fragment.getOEOffsetsBeginPtr();
        nbrUnitEleSize = nbrUnit.elementSize();
        nbrUnitInitAddress = nbrUnit.getAddress();
        innerVerticesNum = this.fragment.getInnerVerticesNum();

        logger.info(
                "Nbrunit: [{}], offsetbeginPtr fist: [{}], end [{}]",
                nbrUnit,
                offsetBeginPtrFirstAddr,
                offsetEndPtrFirstAddr);
        logger.info(
                "nbr unit element size: {}, init address {}",
                nbrUnit.elementSize(),
                nbrUnit.getAddress());
        Long fragVnum = (Long) fragment.getVerticesNum();
        oids = Lists.newArrayListWithCapacity(fragVnum.intValue());
        {
            if (conf.getGrapeVidClass().equals(Long.class)) {
                Vertex<Long> longVertex = (Vertex<Long>) grapeVertex;
                for (long vid = 0; vid < fragVnum.intValue(); ++vid) {
                    longVertex.SetValue(vid);
                    oids.add(fragment.getId((Vertex<GRAPE_VID_T>) longVertex));
                }
            } else if (conf.getGrapeVidClass().equals(Integer.class)) {
                Vertex<Integer> intVertex = (Vertex<Integer>) grapeVertex;
                for (int vid = 0; vid < fragVnum.intValue(); ++vid) {
                    intVertex.SetValue(vid);
                    oids.add(fragment.getId((Vertex<GRAPE_VID_T>) intVertex));
                }
            } else {
                throw new IllegalStateException("grape_OID_t shoule be either int or long");
            }
        }
        edgeIterable = new GenericEdgeIterable(this.fragment.getEdataArrayAccessor());
    }

    /**
     * Get the number of outgoing edges on a vertex with its lid.
     *
     * @param lid
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges(long lid) {
        long oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + lid * 8);
        long oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + lid * 8);
        return (int) (oeEndOffset - oeBeginOffset);
    }

    /**
     * Get a read-only view of the out-edges of this vertex. Note: edge objects returned by this
     * iterable may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge almost always leads to undesired behavior. Accessing the edges with
     * other methods (e.g., addEdge()) during iteration leads to undefined behavior.
     *
     * @param lid
     * @return the out edges (sort order determined by subclass implementation).
     */
    @Override
    public Iterable<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> getEdges(long lid) {
        edgeIterable.setLid(lid);
        return edgeIterable;
    }

    /**
     * Set the outgoing edges for this vertex.
     *
     * @param edges Iterable of edges
     */
    @Override
    public void setEdges(Iterable<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> edges) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Get an iterable of out-edges that can be modified in-place. This can mean changing the
     * current edge value or removing the current edge (by using the iterator version). Note:
     * accessing the edges with other methods (e.g., addEdge()) during iteration leads to undefined
     * behavior.
     *
     * @return An iterable of mutable out-edges
     */
    @Override
    public Iterable<MutableEdge<GIRAPH_OID_T, GIRAPH_EDATA_T>> getMutableEdges() {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Return the value of the first edge with the given target vertex id, or null if there is no
     * such edge. Note: edge value objects returned by this method may be invalidated by the next
     * call. Thus, keeping a reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return edge value (or null if missing)
     */
    @Override
    public GIRAPH_EDATA_T getEdgeValue(long lid, GIRAPH_OID_T targetVertexId) {
        Iterable<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> edges = getEdges(lid);
        for (Edge<GIRAPH_OID_T, GIRAPH_EDATA_T> edge : edges) {
            if (edge.getTargetVertexId().equals(targetVertexId)) {
                return edge.getValue();
            }
        }
        return null;
    }

    /**
     * If an edge to the target vertex exists, set it to the given edge value. This only makes sense
     * with strict graphs.
     *
     * @param targetVertexId Target vertex id
     * @param edgeValue      edge value
     */
    @Override
    public void setEdgeValue(GIRAPH_OID_T targetVertexId, GIRAPH_EDATA_T edgeValue) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Get an iterable over the values of all edges with the given target vertex id. This only makes
     * sense for multigraphs (i.e. graphs with parallel edges). Note: edge value objects returned by
     * this method may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return Iterable of edge values
     */
    @Override
    public Iterable<GIRAPH_EDATA_T> getAllEdgeValues(GIRAPH_OID_T targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to add
     */
    @Override
    public void addEdge(Edge<GIRAPH_OID_T, GIRAPH_EDATA_T> edge) {
        throw new IllegalStateException("not implemented");
    }

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    @Override
    public void removeEdges(GIRAPH_OID_T targetVertexId) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void unwrapMutableEdges() {
        throw new IllegalStateException("not implemented");
    }

    private int grapeEdata2Int() {
        if (conf.getGrapeEdataClass().equals(Long.class)) {
            logger.info("edata: Long");
            return 0;
        } else if (conf.getGrapeEdataClass().equals(Integer.class)) {
            logger.info("edata: Int");
            return 1;
        } else if (conf.getGrapeEdataClass().equals(Double.class)) {
            logger.info("edata: Double");
            return 2;
        } else if (conf.getGrapeEdataClass().equals(Float.class)) {
            logger.info("edata: Float");
            return 3;
        } else if (conf.getGrapeEdataClass().equals(String.class)) {
            logger.info("edata: String");
            return 4;
        }
        throw new IllegalStateException("Cannot recognize edata type " + conf.getGrapeEdataClass());
    }

    public interface ImmutableEdgeIterator<
                    EDGE_OID_T extends WritableComparable, EDGE_DATA_T extends Writable>
            extends Iterator<Edge<EDGE_OID_T, EDGE_DATA_T>> {

        void setLid(int lid);
    }

    public class OnHeapEdgeIterator implements ImmutableEdgeIterator {

        private sun.misc.Unsafe unsafe = JavaRuntime.UNSAFE;
        private DefaultEdge<GIRAPH_OID_T, GIRAPH_EDATA_T> edge = new DefaultEdge<>();
        private long[] nbrUnitAddrs, numOfEdges;
        private GIRAPH_OID_T[] dstOids;
        private GIRAPH_EDATA_T[] edatas;
        private long numEdge, totalNumOfEdges;
        private int nbrPos;
        private int[] nbrPositions;

        public OnHeapEdgeIterator(TypedArray<GRAPE_EDATA_T> edataArray) {
            totalNumOfEdges = getTotalNumOfEdges();
            nbrUnitAddrs = new long[(int) innerVerticesNum];
            numOfEdges = new long[(int) innerVerticesNum];
            nbrPositions = new int[(int) innerVerticesNum];
            // marks the mapping between lid to start pos of nbr, i.e. offset.
            // the reason why we don't resuse oeBegin Offset is that eid may not sequential.
            edatas = (GIRAPH_EDATA_T[]) new Writable[((int) totalNumOfEdges)];
            dstOids = (GIRAPH_OID_T[]) new WritableComparable[((int) totalNumOfEdges)];
            try {
                initArrays(edataArray);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private long getTotalNumOfEdges() {
            long largest =
                    JavaRuntime.getLong(
                            offsetEndPtrFirstAddr + ((innerVerticesNum - 1) << VID_SHIFT_BITS));
            long smallest = JavaRuntime.getLong(offsetBeginPtrFirstAddr + (0 << VID_SHIFT_BITS));
            return largest - smallest;
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
        public Edge<GIRAPH_OID_T, GIRAPH_EDATA_T> next() {
            edge.setTargetVertexId(dstOids[nbrPos]);
            edge.setValue(edatas[nbrPos++]);
            numEdge -= 1;
            return edge;
        }

        private void initArrays(TypedArray<GRAPE_EDATA_T> edataArray) throws IOException {
            int tmpSum = 0;
            long oeBeginOffset, oeEndOffset;
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
                long lidInAddr = (lid << VID_SHIFT_BITS);
                oeBeginOffset = JavaRuntime.getLong(offsetBeginPtrFirstAddr + lidInAddr);
                oeEndOffset = JavaRuntime.getLong(offsetEndPtrFirstAddr + lidInAddr);
                nbrUnitAddrs[(int) lid] = nbrUnitInitAddress + (oeBeginOffset * nbrUnitEleSize);
                numOfEdges[(int) lid] = oeEndOffset - oeBeginOffset;
                tmpSum += numOfEdges[(int) lid];
            }
            if (tmpSum != totalNumOfEdges) {
                throw new IllegalStateException("not equal: " + tmpSum + ", " + totalNumOfEdges);
            }

            // deserialize back from csr.
            int index = 0;
            FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
            int edataType = grapeEdata2Int();
            for (int lid = 0; lid < innerVerticesNum; ++lid) {
                long curAddrr = nbrUnitAddrs[lid];
                nbrPositions[lid] = index;
                for (int j = 0; j < numOfEdges[lid]; ++j) {
                    if (vid_t == 0) {
                        dstOids[index++] = vertexIdManager.getId(unsafe.getLong(curAddrr));
                    } else {
                        dstOids[index++] = vertexIdManager.getId(unsafe.getInt(curAddrr));
                    }

                    long eid = unsafe.getLong(curAddrr + VID_SIZE_IN_BYTE);
                    GRAPE_EDATA_T edata = edataArray.get(eid);
                    switch (edataType) {
                        case 0:
                            Long longValue = (Long) edata;
                            outputStream.writeLong(longValue);
                            break;
                        case 1:
                            Integer intValue = (Integer) edata;
                            outputStream.writeInt(intValue);
                            break;
                        case 2:
                            Double doubleValue = (Double) edata;
                            outputStream.writeDouble(doubleValue);
                            break;
                        case 3:
                            Float floatValue = (Float) edata;
                            outputStream.writeFloat(floatValue);
                            break;
                        case 4:
                            String strValue = (String) edata;
                            // Write raw bytes not utf
                            outputStream.writeBytes(strValue);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected edata type: " + edataType);
                    }
                    curAddrr += nbrUnitEleSize;
                }
            }
            outputStream.finishSetting();
            logger.info("Finish creating stream");

            FFIByteVectorInputStream inputStream =
                    new FFIByteVectorInputStream(outputStream.getVector());
            int index2 = 0;
            while (inputStream.longAvailable() > 0) {
                GIRAPH_EDATA_T edata =
                        (GIRAPH_EDATA_T) ReflectionUtils.newInstance(conf.getEdgeValueClass());
                edata.readFields(inputStream);
                edatas[index2++] = edata;
            }
            if (index2 != totalNumOfEdges) {
                throw new IllegalStateException("Inconsistency occurs in reading vertex data");
            }
            logger.info("Finish creating edata array");
            inputStream.getVector().delete();
        }
    }

    public class GenericEdgeIterable implements Iterable<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> {

        private ImmutableEdgeIterator iterator;

        public GenericEdgeIterable(TypedArray<GRAPE_EDATA_T> edataArray) {
            iterator = new OnHeapEdgeIterator(edataArray);
        }

        public void setLid(long lid) {
            this.iterator.setLid((int) lid);
        }

        @Override
        public Iterator<Edge<GIRAPH_OID_T, GIRAPH_EDATA_T>> iterator() {
            return iterator;
        }
    }
}

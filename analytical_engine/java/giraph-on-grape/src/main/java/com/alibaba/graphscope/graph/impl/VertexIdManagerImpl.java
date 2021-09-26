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

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.VertexIdManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation for vertexId management.
 *
 * @param <OID_T>         giraph vertex id type
 * @param <GRAPE_OID_T>   grape vertex oid
 * @param <GRAPE_VID_T>   grape vertex vid
 * @param <GRAPE_VDATA_T> grape vertex data
 * @param <GRAPE_EDATA_T> grape edge data
 */
public class VertexIdManagerImpl<
                OID_T extends WritableComparable,
                GRAPE_OID_T,
                GRAPE_VID_T,
                GRAPE_VDATA_T,
                GRAPE_EDATA_T>
        implements VertexIdManager<GRAPE_VID_T, OID_T> {

    private static Logger logger = LoggerFactory.getLogger(VertexIdManagerImpl.class);

    private IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private long vertexNum;
    private List<OID_T> vertexIdList;
    private Object2ObjectArrayMap oid2lid;
    private ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf;

    /**
     * To provide giraph users with all oids, we need to get all oids out of c++ memory, then let
     * java read the stream.
     *
     * @param fragment  fragment
     * @param vertexNum number of vertices
     * @param conf      configuration to use.
     */
    public VertexIdManagerImpl(
            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            long vertexNum,
            ImmutableClassesGiraphConfiguration<OID_T, ?, ?> conf) {
        this.fragment = fragment;
        this.vertexNum = vertexNum;
        this.conf = conf;
        vertexIdList = new ArrayList<OID_T>((int) vertexNum);
        oid2lid = new Object2ObjectArrayMap((int) vertexNum);

        FFIByteVectorInputStream inputStream = generateVertexIdStream();
        try {
            if (conf.getGrapeVidClass().equals(Long.class)) {
                for (long i = 0; i < vertexNum; ++i) {
                    WritableComparable oid = conf.createVertexId();
                    oid.readFields(inputStream);
                    vertexIdList.add((OID_T) oid);
                    oid2lid.put(oid, i);
                }
            } else {
                for (int i = 0; i < vertexNum; ++i) {
                    WritableComparable oid = conf.createVertexId();
                    oid.readFields(inputStream);
                    vertexIdList.add((OID_T) oid);
                    oid2lid.put(oid, i);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        inputStream.clear();
    }

    @Override
    public OID_T getId(long lid) {
        return vertexIdList.get((int) lid);
    }

    @Override
    public GRAPE_VID_T getLid(OID_T oid) {
        return (GRAPE_VID_T) oid2lid.get(oid);
    }

    private FFIByteVectorInputStream generateVertexIdStream() {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        Iterable<Vertex<GRAPE_VID_T>> vertexIterable;
        if (conf.getGrapeVidClass().equals(Long.class)) {
            vertexIterable = fragment.vertices().longIterable();
        } else {
            vertexIterable = fragment.vertices().intIterable();
        }
        try {
            if (conf.getGrapeOidClass().equals(Long.class)) {
                for (Vertex<GRAPE_VID_T> vertex : vertexIterable) {
                    Long value = (Long) fragment.getId(vertex);
                    outputStream.writeLong(value);
                }
            } else if (conf.getGrapeOidClass().equals(Integer.class)) {
                for (Vertex<GRAPE_VID_T> vertex : vertexIterable) {
                    Integer value = (Integer) fragment.getId(vertex);
                    outputStream.writeInt(value);
                }
            } else if (conf.getGrapeOidClass().equals(Double.class)) {
                for (Vertex<GRAPE_VID_T> vertex : vertexIterable) {
                    Double value = (Double) fragment.getId(vertex);
                    outputStream.writeDouble(value);
                }
            } else if (conf.getGrapeOidClass().equals(Float.class)) {
                for (Vertex<GRAPE_VID_T> vertex : vertexIterable) {
                    Float value = (Float) fragment.getId(vertex);
                    outputStream.writeFloat(value);
                }
            } else {
                logger.error("Unsupported oid class: " + conf.getGrapeOidClass().getName());
            }
            outputStream.finishSetting();
            logger.info(
                    "Vertex data stream size: "
                            + outputStream.bytesWriten()
                            + ", vertices: "
                            + vertexNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new FFIByteVectorInputStream(outputStream.getVector());
    }
}

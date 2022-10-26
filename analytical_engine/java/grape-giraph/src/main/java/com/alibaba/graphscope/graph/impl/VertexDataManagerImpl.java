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
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation for vertex data management. Basically we retrieve all vdata from c++
 * fragment, and store in a java list.
 *
 * @param <VDATA_T>            giraph vertex data type
 * @param <GRAPE_OID_T>        grape vertex oid
 * @param <GRAPE_VID_T>        grape vertex vid
 * @param <GRAPE_VDATA_T>grape vertex data
 * @param <GRAPE_EDATA_T>grape edge data
 */
public class VertexDataManagerImpl<
                VDATA_T extends Writable, GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
        implements VertexDataManager<VDATA_T> {

    private static Logger logger = LoggerFactory.getLogger(VertexDataManagerImpl.class);

    private IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private List<VDATA_T> vertexDataList;
    private long vertexNum;
    private ImmutableClassesGiraphConfiguration<?, VDATA_T, ?> conf;

    public VertexDataManagerImpl(
            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            long vertexNum,
            ImmutableClassesGiraphConfiguration<?, VDATA_T, ?> configuration) {
        this.fragment = fragment;
        this.vertexNum = vertexNum;
        vertexDataList = new ArrayList<VDATA_T>((int) vertexNum);
        this.conf = configuration;

        FFIByteVectorInputStream inputStream = generateVertexDataStream();

        try {
            for (int i = 0; i < vertexNum; ++i) {
                VDATA_T vdata = conf.createVertexValue();
                vdata.readFields(inputStream);
                vertexDataList.add(vdata);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        inputStream.clear();
    }

    @Override
    public VDATA_T getVertexData(long lid) {
        checkLid(lid);
        return vertexDataList.get((int) lid);
    }

    /**
     * When set a value, we don't know this param will or will not be reused, so copy the value.
     *
     * @param lid        lid
     * @param vertexData value to set.
     */
    @Override
    public void setVertexData(long lid, VDATA_T vertexData) {
        checkLid(lid);
        vertexDataList.set((int) lid, vertexData);
    }

    private void checkLid(long lid) {
        if (lid >= vertexNum) {
            logger.error("Querying lid out of range: " + lid + " max lid: " + lid);
            throw new RuntimeException("Vertex of range: " + lid + " max possible: " + lid);
        }
    }

    private FFIByteVectorInputStream generateVertexDataStream() {
        FFIByteVectorOutputStream outputStream = new FFIByteVectorOutputStream();
        readVertexDataFromIFragment(outputStream);

        return new FFIByteVectorInputStream(outputStream.getVector());
    }

    private void readVertexDataFromIFragment(FFIByteVectorOutputStream outputStream) {
        try {
            // We need to form all vdata as a stream, so java writables can read from this stream.
            Iterable<Vertex<GRAPE_VID_T>> iterable;
            if (conf.getGrapeVidClass().equals(Long.class)) {
                iterable = (Iterable<Vertex<GRAPE_VID_T>>) fragment.vertices().longIterable();
            } else if (conf.getGrapeVidClass().equals(Integer.class)) {
                iterable = (Iterable<Vertex<GRAPE_VID_T>>) fragment.vertices().intIterable();
            } else {
                throw new IllegalStateException(
                        "No recognizable vid" + conf.getGrapeVidClass().getName());
            }
            if (conf.getGrapeVdataClass().equals(Long.class)) {
                for (Vertex<GRAPE_VID_T> vertex : iterable) {
                    Long value = (Long) fragment.getData(vertex);
                    outputStream.writeLong(value);
                }
            } else if (conf.getGrapeVdataClass().equals(Integer.class)) {
                for (Vertex<GRAPE_VID_T> vertex : iterable) {
                    Integer value = (Integer) fragment.getData(vertex);
                    outputStream.writeInt(value);
                }
            } else if (conf.getGrapeVdataClass().equals(Double.class)) {
                for (Vertex<GRAPE_VID_T> vertex : iterable) {
                    Double value = (Double) fragment.getData(vertex);
                    outputStream.writeDouble(value);
                }
            } else if (conf.getGrapeVdataClass().equals(Float.class)) {
                for (Vertex<GRAPE_VID_T> vertex : iterable) {
                    Float value = (Float) fragment.getData(vertex);
                    outputStream.writeFloat(value);
                }
            } else if (conf.getGrapeVdataClass().equals(String.class)) {
                for (Vertex<GRAPE_VID_T> vertex : iterable) {
                    String value = (String) fragment.getData(vertex);
                    outputStream.writeBytes(value);
                }
            } else {
                logger.error("Unsupported oid class: " + conf.getGrapeOidClass().getName());
            }
            // else if (conf.getGrapeVdataClass().equals the userDefined class...
            outputStream.finishSetting();
            logger.info(
                    "Vertex data stream size: "
                            + outputStream.bytesWriten()
                            + ", vertices: "
                            + vertexNum);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

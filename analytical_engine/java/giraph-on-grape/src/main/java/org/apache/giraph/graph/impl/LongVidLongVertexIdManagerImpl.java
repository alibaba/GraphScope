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
package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;

import it.unimi.dsi.fastutil.longs.Long2LongArrayMap;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexIdManager;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specialized implementation for long oid. Don't store values, create obj when needed.
 *
 * @param <GRAPE_OID_T>        grape oid
 * @param <GRAPE_VDATA_T>      grape vdata
 * @param <GRAPE_EDATA_T>grape edata
 */
public class LongVidLongVertexIdManagerImpl<GRAPE_OID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
        implements VertexIdManager<Long, LongWritable> {

    private static Logger logger = LoggerFactory.getLogger(LongVidLongVertexIdManagerImpl.class);

    private IFragment<GRAPE_OID_T, Long, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private long vertexNum;
    //    private List<OID_T> vertexIdList;
    private ImmutableClassesGiraphConfiguration<? super LongWritable, ?, ?> conf;
    private Vertex<Long> grapeVertex;
    private LongWritable[] vertexIds;
    private Long2LongArrayMap oid2Lid;

    public LongVidLongVertexIdManagerImpl(
            IFragment<GRAPE_OID_T, Long, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            long vertexNum,
            ImmutableClassesGiraphConfiguration<? super LongWritable, ?, ?> conf) {
        this.fragment = fragment;
        this.vertexNum = vertexNum; // fragment vertex Num
        this.conf = conf;

        if (!conf.getGrapeVidClass().equals(Long.class)) {
            throw new IllegalStateException(
                    "LongVertexIdManager expect the fragment using long as oid");
        }
        if (fragment.vertices().size() != vertexNum) {
            throw new IllegalStateException(
                    "inner vertices size not equal to vertex num"
                            + fragment.getVerticesNum()
                            + ", "
                            + vertexNum);
        }

        grapeVertex = FFITypeFactoryhelper.newVertex(Long.class);
        grapeVertex.SetValue(0L);

        vertexIds = new LongWritable[(int) vertexNum];
        oid2Lid = new Long2LongArrayMap((int) vertexNum);
        int index = 0;
        for (Vertex<Long> vertex : fragment.vertices().longIterable()) {
            Long value = (Long) fragment.getId(vertex);
            oid2Lid.put(value, index);
            vertexIds[index++] = new LongWritable(value);
        }
    }

    @Override
    public LongWritable getId(long lid) {
        checkLid(lid);
        return vertexIds[(int) lid];
    }

    @Override
    public Long getLid(LongWritable oid) {
        return oid2Lid.get(oid.get());
    }

    private void checkLid(long lid) {
        if (lid >= vertexNum) {
            logger.error("Querying lid out of range: " + lid + " max lid: " + lid);
            throw new RuntimeException("Vertex of range: " + lid + " max possible: " + lid);
        }
    }
}

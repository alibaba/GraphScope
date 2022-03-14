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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexDataManager;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vertex data type = doubleWritable
 *
 * @param <GRAPE_OID_T>
 * @param <GRAPE_VID_T>
 * @param <GRAPE_VDATA_T>
 * @param <GRAPE_EDATA_T>
 */
public class LongVidLongVertexDataManager<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T>
        implements VertexDataManager<LongWritable> {

    private static Logger logger = LoggerFactory.getLogger(LongVidLongVertexDataManager.class);

    private IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment;
    private long vertexNum;
    private ImmutableClassesGiraphConfiguration<?, ? super LongWritable, ?> conf;
    private Vertex<Long> grapeVertex;
    private LongWritable[] vdatas;

    /**
     * @param fragment
     * @param vertexNum     should be inner vertices num.
     * @param configuration
     */
    public LongVidLongVertexDataManager(
            IFragment<GRAPE_OID_T, GRAPE_VID_T, GRAPE_VDATA_T, GRAPE_EDATA_T> fragment,
            long vertexNum,
            ImmutableClassesGiraphConfiguration<?, ? super LongWritable, ?> configuration) {
        this.fragment = fragment;
        this.vertexNum = vertexNum;
        this.conf = configuration;

        if (!conf.getGrapeVidClass().equals(Long.class)
                || !conf.getGrapeVdataClass().equals(Long.class)) {
            throw new IllegalStateException("Expect fragment with long vid and double vdata");
        }
        grapeVertex = FFITypeFactoryhelper.newVertex(Long.class);
        grapeVertex.SetValue(0L);

        vdatas = new LongWritable[(int) vertexNum];

        if (fragment.innerVertices().size() != vertexNum) {
            throw new IllegalStateException(
                    "expect frag inner vertices size equal to vertex Num"
                            + fragment.innerVertices().size()
                            + ", "
                            + vertexNum);
        }
        int index = 0;
        for (Vertex<GRAPE_VID_T> vertex : fragment.innerVertices().longIterable()) {
            Long value = (Long) fragment.getData(vertex);
            vdatas[index++] = new LongWritable(value);
        }
    }

    @Override
    public LongWritable getVertexData(long lid) {
        checkLid(lid);
        return vdatas[(int) lid];
    }

    @Override
    public void setVertexData(long lid, LongWritable vertexData) {
        checkLid(lid);
        vdatas[(int) lid].set(vertexData.get());
    }

    private void checkLid(long lid) {
        if (lid >= vertexNum) {
            throw new RuntimeException("Vertex of range: " + lid + " max possible: " + lid);
        }
    }
}

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

package com.alibaba.graphscope.example.property.sssp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.ContextDataType;
import com.alibaba.graphscope.context.LabeledVertexPropertyContext;
import com.alibaba.graphscope.context.PropertyDefaultContextBase;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.VertexSet;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.PropertyMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link com.alibaba.graphscope.context.LabeledVertexPropertyContext}. By
 * calling addColumn, you can create a vertex data vector, and label it with a <em>property</em>.
 */
public class PropertySSSPVertexPropertyContext extends LabeledVertexPropertyContext<Long>
        implements PropertyDefaultContextBase<Long> {
    private static Logger logger =
            LoggerFactory.getLogger(PropertySSSPVertexPropertyContext.class.getName());
    public List<VertexSet> curModified;
    public List<VertexSet> nextModified;
    public List<GSVertexArray<Double>> partialResults;
    public List<Long> distColumnIndices;
    public long sourceOid;

    /**
     * Init the context by created inner context and create data structures holding runtime data.
     *
     * @param fragment fragment bound to this context.
     * @param messageManager manage messages.
     * @param jsonObject contains the user-defined parameters in json manner.
     */
    @Override
    public void init(
            ArrowFragment<Long> fragment,
            PropertyMessageManager messageManager,
            JSONObject jsonObject) {
        // must be called
        createFFIContext(fragment);
        logger.info("params size " + jsonObject.size() + ", " + jsonObject.toJSONString());
        int labelNum = fragment.vertexLabelNum();

        curModified = new ArrayList<>(labelNum);
        nextModified = new ArrayList<>(labelNum);
        partialResults = new ArrayList<>(labelNum);
        distColumnIndices = new ArrayList<>(labelNum);
        for (int i = 0; i < labelNum; ++i) {
            VertexRange<Long> vertices = fragment.vertices(i);
            curModified.add(new VertexSet(vertices));
            nextModified.add(new VertexSet(vertices));
            partialResults.add(FFITypeFactoryhelper.newGSVertexArray(Double.class));
            partialResults.get(i).init(fragment.vertices(i), Double.MAX_VALUE);
            distColumnIndices.add(addColumn(i, "dist_" + i, ContextDataType.kDouble));
            logger.info(
                    "range "
                            + partialResults.get(i).GetVertexRange().begin().GetValue()
                            + ", "
                            + partialResults.get(i).GetVertexRange().end().GetValue());
        }
        sourceOid = jsonObject.getLong("src");
        if (Objects.isNull(sourceOid)) {
            logger.error("source Oid not set in parameter.");
            return;
        }
    }
}

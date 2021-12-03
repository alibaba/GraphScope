/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.server.processor;

import com.alibaba.maxgraph.compiler.api.schema.GraphSchema;
import com.alibaba.maxgraph.rpc.TimelyResultProcessor;

import java.util.Map;

public abstract class AbstractResultProcessor implements TimelyResultProcessor {
    protected GremlinResultTransform gremlinResultTransform;
    protected Map<Integer, String> labelIndexNameList;
    protected GraphSchema schema;

    public void setResultTransform(GremlinResultTransform resultTransform) {
        this.gremlinResultTransform = resultTransform;
    }

    public void setLabelIndexNameList(Map<Integer, String> labelIndexNameList) {
        this.labelIndexNameList = labelIndexNameList;
    }

    public void setSchema(GraphSchema schema) {
        this.schema = schema;
    }
}

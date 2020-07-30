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
package com.alibaba.maxgraph.sdkcommon.compiler.custom.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public class RangeSumFunction<VALUE> implements Function<Traverser<VALUE>, Traverser<Map<VALUE, Double>>>, Serializable {
    private static final long serialVersionUID = -6037294006085152014L;

    private String propName;
    private int start;
    private int count;

    public RangeSumFunction(String propName, int start, int count) {
        checkArgument(start >= 0, "start in rangeSum must >= 0");
        checkArgument(count > 0, "start in rangeSum must > 0");
        this.propName = propName;
        this.start = start;
        this.count = count;
    }

    @Override
    public Traverser<Map<VALUE, Double>> apply(Traverser<VALUE> valueTraverser) {
        throw new UnsupportedOperationException();
    }

    public String getPropName() {
        return this.propName;
    }

    public int getStart() {
        return this.start;
    }

    public int getCount() {
        return this.count;
    }
}

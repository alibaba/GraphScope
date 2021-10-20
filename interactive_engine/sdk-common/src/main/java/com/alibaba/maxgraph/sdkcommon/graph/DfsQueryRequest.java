/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.sdkcommon.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;

public class DfsQueryRequest implements DfsRequest {
    private Bytecode bytecode;
    private long start;
    private long end;
    private long batchSize;
    private boolean order;

    public DfsQueryRequest() {}

    public DfsQueryRequest(Bytecode bytecode, long start, long end, long batchSize, boolean order) {
        this.bytecode = bytecode;
        this.start = start;
        this.end = end;
        this.batchSize = batchSize;
        this.order = order;
    }

    @Override
    public Bytecode getBytecode() {
        return bytecode;
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getEnd() {
        return end;
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

    @Override
    public boolean isOrder() {
        return order;
    }

    @Override
    public void setTraversal(Object traversal) {}
}

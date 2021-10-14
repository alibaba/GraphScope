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
package com.alibaba.graphscope.groot;

import com.alibaba.graphscope.groot.schema.GraphDef;

public class SnapshotWithSchema {

    private long snapshotId;
    private GraphDef graphDef;

    public SnapshotWithSchema(long snapshotId, GraphDef graphDef) {
        this.snapshotId = snapshotId;
        this.graphDef = graphDef;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public GraphDef getGraphDef() {
        return graphDef;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(SnapshotWithSchema snapshotWithSchema) {
        return new Builder(snapshotWithSchema);
    }

    public static class Builder {

        private long snapshotId;
        private GraphDef graphDef;

        public Builder() {
            this.snapshotId = -1L;
        }

        public Builder(SnapshotWithSchema snapshotWithSchema) {
            this.snapshotId = snapshotWithSchema.getSnapshotId();
            this.graphDef = snapshotWithSchema.getGraphDef();
        }

        public Builder setSnapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder setGraphDef(GraphDef graphDef) {
            this.graphDef = graphDef;
            return this;
        }

        public SnapshotWithSchema build() {
            return new SnapshotWithSchema(snapshotId, graphDef);
        }
    }
}

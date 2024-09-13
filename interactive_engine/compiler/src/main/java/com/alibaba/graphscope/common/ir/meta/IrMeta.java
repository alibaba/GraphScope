/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta;

import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;

import java.util.Objects;

/**
 * This class describes basic metadata information required by IR, including Schema, Procedures.
 * They are versioned using the same snapshot id.
 */
public class IrMeta {
    protected final GraphId graphId;
    protected final SnapshotId snapshotId;
    protected final IrGraphSchema schema;
    protected final GraphStoredProcedures storedProcedures;

    public IrMeta(IrGraphSchema schema) {
        this(GraphId.DEFAULT, SnapshotId.createEmpty(), schema, new GraphStoredProcedures());
    }

    public IrMeta(IrGraphSchema schema, GraphStoredProcedures storedProcedures) {
        this(GraphId.DEFAULT, SnapshotId.createEmpty(), schema, storedProcedures);
    }

    public IrMeta(SnapshotId snapshotId, IrGraphSchema schema) {
        this(GraphId.DEFAULT, snapshotId, schema, new GraphStoredProcedures());
    }

    public IrMeta(
            GraphId graphId,
            SnapshotId snapshotId,
            IrGraphSchema schema,
            GraphStoredProcedures storedProcedures) {
        this.graphId = graphId;
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.schema = Objects.requireNonNull(schema);
        this.storedProcedures = Objects.requireNonNull(storedProcedures);
    }

    public IrGraphSchema getSchema() {
        return schema;
    }

    public SnapshotId getSnapshotId() {
        return snapshotId;
    }

    public GraphStoredProcedures getStoredProcedures() {
        return storedProcedures;
    }

    public GraphId getGraphId() {
        return graphId;
    }
}

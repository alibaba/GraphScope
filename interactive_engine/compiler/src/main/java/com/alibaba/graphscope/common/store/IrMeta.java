/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.store;

import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedures;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.Objects;

public class IrMeta {
    private final SnapshotId snapshotId;
    private final IrGraphSchema schema;
    private final @Nullable StoredProcedures storedProcedures;

    public IrMeta(IrGraphSchema schema) throws IOException {
        this(SnapshotId.createEmpty(), schema);
    }

    public IrMeta(IrGraphSchema schema, StoredProcedures storedProcedures) {
        this(SnapshotId.createEmpty(), schema, storedProcedures);
    }

    public IrMeta(SnapshotId snapshotId, IrGraphSchema schema) throws IOException {
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.schema = Objects.requireNonNull(schema);
        this.storedProcedures = null;
    }

    public IrMeta(SnapshotId snapshotId, IrGraphSchema schema, StoredProcedures storedProcedures) {
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

    public @Nullable StoredProcedures getStoredProcedures() {
        return storedProcedures;
    }
}

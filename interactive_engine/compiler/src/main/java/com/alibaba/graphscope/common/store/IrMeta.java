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

import com.alibaba.graphscope.common.ir.schema.StatisticSchema;

import java.util.Objects;

public class IrMeta {
    private final SnapshotId snapshotId;
    private StatisticSchema schema;

    public IrMeta(StatisticSchema schema) {
        this(SnapshotId.createEmpty(), schema);
    }

    public IrMeta(SnapshotId snapshotId, StatisticSchema schema) {
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.schema = Objects.requireNonNull(schema);
    }

    public StatisticSchema getSchema() {
        return schema;
    }

    public SnapshotId getSnapshotId() {
        return snapshotId;
    }
}

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

public class SnapshotId {
    public static final SnapshotId createEmpty() {
        return new SnapshotId(false, -1);
    }
    private final boolean acquired;
    private final long snapshotId;
    public SnapshotId(boolean acquired, long snapshotId) {
        this.acquired = acquired;
        this.snapshotId = snapshotId;
    }

    public boolean isAcquired() {
        return acquired;
    }

    public long getSnapshotId() {
        return snapshotId;
    }
}

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
package com.alibaba.graphscope.groot.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SnapshotInfo implements Comparable<SnapshotInfo> {

    private final long snapshotId;

    private final long ddlSnapshotId;

    @JsonCreator
    public SnapshotInfo(
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("ddlSnapshotId") long ddlSnapshotId) {
        this.snapshotId = snapshotId;
        this.ddlSnapshotId = ddlSnapshotId;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getDdlSnapshotId() {
        return ddlSnapshotId;
    }

    @Override
    public int compareTo(SnapshotInfo o) {
        return Long.compare(snapshotId, o.snapshotId);
    }

    @Override
    public String toString() {
        return "SnapshotInfo{"
                + "snapshotId="
                + snapshotId
                + ", ddlSnapshotId="
                + ddlSnapshotId
                + '}';
    }
}

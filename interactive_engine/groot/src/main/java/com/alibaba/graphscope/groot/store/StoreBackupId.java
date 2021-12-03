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
package com.alibaba.graphscope.groot.store;

import com.alibaba.maxgraph.proto.groot.StoreBackupIdPb;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class StoreBackupId {

    private int globalBackupId;
    private Map<Integer, Integer> partitionToBackupId;

    public StoreBackupId(int globalBackupId) {
        this.globalBackupId = globalBackupId;
        this.partitionToBackupId = new HashMap<>();
    }

    public StoreBackupId(int globalBackupId, Map<Integer, Integer> partitionToBackupId) {
        this.globalBackupId = globalBackupId;
        this.partitionToBackupId = partitionToBackupId;
    }

    public int getGlobalBackupId() {
        return globalBackupId;
    }

    public Map<Integer, Integer> getPartitionToBackupId() {
        return partitionToBackupId;
    }

    public void addPartitionBackupId(int partitionId, int partitionBackupId) {
        this.partitionToBackupId.put(partitionId, partitionBackupId);
    }

    public static StoreBackupId parseProto(StoreBackupIdPb proto) {
        int globalBackupId = proto.getGlobalBackupId();
        Map<Integer, Integer> partitionToBackupId = proto.getPartitionToBackupIdMap();
        return new StoreBackupId(globalBackupId, partitionToBackupId);
    }

    public StoreBackupIdPb toProto() {
        return StoreBackupIdPb.newBuilder()
                .setGlobalBackupId(globalBackupId)
                .putAllPartitionToBackupId(partitionToBackupId)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoreBackupId storeBackupId = (StoreBackupId) o;
        if (!partitionToBackupId.equals(storeBackupId.partitionToBackupId)) {
            return false;
        }
        return (globalBackupId == storeBackupId.globalBackupId);
    }

    @Override
    public int hashCode() {
        return globalBackupId;
    }
}

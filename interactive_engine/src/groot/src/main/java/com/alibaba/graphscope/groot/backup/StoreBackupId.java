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
package com.alibaba.graphscope.groot.backup;

import com.alibaba.maxgraph.proto.v2.StoreBackupIdPb;

import java.util.List;

public class StoreBackupId {

    private int globalBackupId;
    private List<Integer> partitionBackupIds;

    public StoreBackupId(int globalBackupId, List<Integer> partitionBackupIds) {
        this.globalBackupId = globalBackupId;
        this.partitionBackupIds = partitionBackupIds;
    }

    public int getGlobalBackupId() {
        return globalBackupId;
    }

    public List<Integer> getPartitionBackupIds() {
        return partitionBackupIds;
    }

    public static StoreBackupId parseProto(StoreBackupIdPb proto) {
        int globalBackupId = proto.getGlobalBackupId();
        List<Integer> partitionBackupIds = proto.getPartitionBackupIdsList();
        return new StoreBackupId(globalBackupId, partitionBackupIds);
    }

    public StoreBackupIdPb toProto() {
        return StoreBackupIdPb.newBuilder()
                .setGlobalBackupId(globalBackupId)
                .addAllPartitionBackupIds(partitionBackupIds)
                .build();
    }
}

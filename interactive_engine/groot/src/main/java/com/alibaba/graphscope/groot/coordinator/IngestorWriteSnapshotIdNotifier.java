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

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestorWriteSnapshotIdNotifier implements WriteSnapshotIdNotifier {
    private static final Logger logger =
            LoggerFactory.getLogger(IngestorWriteSnapshotIdNotifier.class);

    private RoleClients<IngestorSnapshotClient> ingestorSnapshotClients;
    private int ingestorCount;

    public IngestorWriteSnapshotIdNotifier(
            Configs configs, RoleClients<IngestorSnapshotClient> ingestorSnapshotClients) {
        this.ingestorSnapshotClients = ingestorSnapshotClients;
        this.ingestorCount = CommonConfig.INGESTOR_NODE_COUNT.get(configs);
    }

    @Override
    public void notifyWriteSnapshotIdChanged(long snapshotId) {
        for (int i = 0; i < this.ingestorCount; i++) {
            try {
                int realtimeWriterId = i;
                this.ingestorSnapshotClients
                        .getClient(realtimeWriterId)
                        .advanceIngestSnapshotId(
                                snapshotId,
                                new CompletionCallback<Long>() {
                                    @Override
                                    public void onCompleted(Long previousSnapshotId) {
                                        if (previousSnapshotId > snapshotId) {
                                            logger.error(
                                                    "unexpected previousSnapshotId ["
                                                            + previousSnapshotId
                                                            + "], "
                                                            + "should <= ["
                                                            + snapshotId
                                                            + "]. "
                                                            + "target realtimeWriter ["
                                                            + realtimeWriterId
                                                            + "]");
                                        }
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        logger.error(
                                                "error in advanceIngestSnapshotId ["
                                                        + snapshotId
                                                        + "]. realtimeWriter ["
                                                        + realtimeWriterId
                                                        + "]",
                                                t);
                                    }
                                });
            } catch (Exception e) {
                logger.warn("update writeSnapshotId failed. realtimeWriter [" + i + "]", e);
            }
        }
    }
}

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
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.rpc.RoleClients;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestorWriteSnapshotIdNotifier {
    private static final Logger logger =
            LoggerFactory.getLogger(IngestorWriteSnapshotIdNotifier.class);

    private final RoleClients<IngestorSnapshotClient> ingestorSnapshotClients;
    private final int frontendCount;

    public IngestorWriteSnapshotIdNotifier(
            Configs configs, RoleClients<IngestorSnapshotClient> ingestorSnapshotClients) {
        this.ingestorSnapshotClients = ingestorSnapshotClients;
        this.frontendCount = CommonConfig.FRONTEND_NODE_COUNT.get(configs);
    }

    public void notifyWriteSnapshotIdChanged(long si) {
        CompletionCallback<Long> callback =
                new CompletionCallback<Long>() {
                    @Override
                    public void onCompleted(Long prev) {
                        if (prev > si) {
                            logger.error("unexpected previous SI {}, should <= {}", prev, si);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("error in advanceIngestSnapshotId {}: {}", si, t.toString());
                    }
                };
        for (int i = 0; i < this.frontendCount; i++) {
            try {
                IngestorSnapshotClient client = ingestorSnapshotClients.getClient(i);
                client.advanceIngestSnapshotId(si, callback);
            } catch (Exception e) {
                logger.warn("update writeSnapshotId failed. realtimeWriter [{}]", i, e);
            }
        }
    }
}

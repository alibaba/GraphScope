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

import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.DiscoveryConfig;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.OperationType;

import java.util.List;

public class Utils {
    public static final OperationBatch MARKER_BATCH =
            OperationBatch.newBuilder()
                    .addOperationBlob(OperationBlob.MARKER_OPERATION_BLOB)
                    .build();

    public static String getHostTemplate(Configs configs, RoleType role) {
        switch (role) {
            case FRONTEND:
                return DiscoveryConfig.DNS_NAME_PREFIX_FRONTEND.get(configs);
            case COORDINATOR:
                return DiscoveryConfig.DNS_NAME_PREFIX_COORDINATOR.get(configs);
            case STORE:
            case GAIA_RPC:
            case GAIA_ENGINE:
                return DiscoveryConfig.DNS_NAME_PREFIX_STORE.get(configs);
            default:
                throw new IllegalArgumentException("invalid role [" + role + "]");
        }
    }

    public static int getPort(Configs configs) {
        String discoveryMode = CommonConfig.DISCOVERY_MODE.get(configs).toLowerCase();
        if (discoveryMode.equals("file")) {
            RoleType role = RoleType.fromName(CommonConfig.ROLE_NAME.get(configs));
            int idx = CommonConfig.NODE_IDX.get(configs);
            return getPort(configs, role, idx);
        } else {
            return CommonConfig.RPC_PORT.get(configs);
        }
    }

    public static int getPort(Configs configs, RoleType role, int idx) {
        String s;
        switch (role) {
            case FRONTEND:
                s = CommonConfig.FRONTEND_RPC_PORT.get(configs);
                break;
            case COORDINATOR:
                s = CommonConfig.COORDINATOR_RPC_PORT.get(configs);
                break;
            case STORE:
                s = CommonConfig.STORE_RPC_PORT.get(configs);
                break;
            case GAIA_RPC:
                s = CommonConfig.GAIA_RPC_PORT.get(configs);
                break;
            case GAIA_ENGINE:
                s = CommonConfig.GAIA_ENGINE_PORT.get(configs);
                break;
            default:
                throw new IllegalArgumentException("invalid role [" + role + "]");
        }
        if (s.isEmpty()) { // For backward compatibility
            return CommonConfig.RPC_PORT.get(configs);
        } else {
            String[] array = s.split(",");
            if (idx >= array.length) {
                // throw new IllegalArgumentException("Invalid index " + idx + " of " + s);
                idx = 0; // Just use the first one. In this case, assume they are in different pods.
            }
            if (array[idx].isEmpty()) {
                throw new IllegalArgumentException("Invalid port " + array[idx] + " of " + role);
            }
            return Integer.parseInt(array[idx]);
        }
    }

    public static OperationBatch extractOperations(
            OperationBatch input, List<OperationType> types) {
        boolean hasOtherType = false;
        for (int i = 0; i < input.getOperationCount(); ++i) {
            OperationBlob blob = input.getOperationBlob(i);
            OperationType opType = blob.getOperationType();
            if (!types.contains(opType)) {
                hasOtherType = true;
                break;
            }
        }
        if (!hasOtherType) {
            return input;
        }
        OperationBatch.Builder batchBuilder = OperationBatch.newBuilder();
        batchBuilder.setLatestSnapshotId(input.getLatestSnapshotId());
        for (int i = 0; i < input.getOperationCount(); ++i) {
            OperationBlob blob = input.getOperationBlob(i);
            OperationType opType = blob.getOperationType();
            if (types.contains(opType)) {
                batchBuilder.addOperationBlob(blob);
            }
        }
        return batchBuilder.build();
    }
}

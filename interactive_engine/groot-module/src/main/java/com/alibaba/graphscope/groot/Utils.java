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
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.OperationType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.List;

public class Utils {
    public static final Logger logger = LoggerFactory.getLogger(Utils.class);
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

    public static boolean fileExists(String name) {
        return new File(name).exists();
    }

    public static boolean fileClosed(String name) {
        String filePath = new File(name).getAbsolutePath();
        try {
            Process proc = new ProcessBuilder("lsof", filePath).start();
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains(filePath)) {
                        return false;
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Exception when checking file {}", filePath, e);
            return false;
        }
        return true;
    }

    // Check if the lock of 0 is available
    public static boolean isLockAvailable(Configs configs) {
        String dataRoot = StoreConfig.STORE_DATA_PATH.get(configs);
        // Get the LOCK file of first partition
        String LOCK = Paths.get(dataRoot, "" + 0, "LOCK").toAbsolutePath().toString();
        if (fileExists(LOCK) && !fileClosed(LOCK)) {
            logger.warn("LOCK {} is unavailable", LOCK);
            return false;
        }
        return true;
    }
}

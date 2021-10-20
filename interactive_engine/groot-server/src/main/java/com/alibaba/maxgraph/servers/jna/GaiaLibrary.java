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
package com.alibaba.maxgraph.servers.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface GaiaLibrary extends Library {
    GaiaLibrary INSTANCE = Native.load("maxgraph_ffi", GaiaLibrary.class);

    Pointer initialize(byte[] config, int len);

    GaiaPortsResponse startEngine(Pointer engine);

    void addPartition(Pointer engine, int partitionId, Pointer graph);

    void updatePartitionRouting(Pointer engine, int partitionId, int serverId);

    void stopEngine(Pointer engine);

    void updatePeerView(Pointer pointer, String peerViewString);

    void dropEnginePortsResponse(GaiaPortsResponse response);
}

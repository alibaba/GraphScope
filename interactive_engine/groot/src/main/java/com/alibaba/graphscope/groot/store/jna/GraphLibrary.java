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
package com.alibaba.graphscope.groot.store.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface GraphLibrary extends Library {
    GraphLibrary INSTANCE = Native.load("maxgraph_ffi", GraphLibrary.class);

    Pointer openGraphStore(byte[] config, int len);

    boolean closeGraphStore(Pointer storePointer);

    JnaResponse writeBatch(Pointer storePointer, long snapshotId, byte[] data, int len);

    JnaResponse getGraphDefBlob(Pointer storePointer);

    JnaResponse ingestData(Pointer storePointer, String dataPath);

    Pointer openGraphBackupEngine(Pointer storePointer, String backupPath);

    void closeGraphBackupEngine(Pointer bePointer);

    JnaResponse createNewBackup(Pointer bePointer);

    JnaResponse deleteBackup(Pointer bePointer, int backupId);

    JnaResponse restoreFromBackup(Pointer bePointer, String restorePath, int backupId);

    JnaResponse verifyBackup(Pointer bePointer, int backupId);

    JnaResponse getBackupList(Pointer bePointer);

    void dropJnaResponse(JnaResponse jnaResponse);

    Pointer createWrapperPartitionGraph(Pointer graphStore);

    void deleteWrapperPartitionGraph(Pointer wrapperPartitionGraph);
}

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
package com.alibaba.graphscope.groot.sdk;

import com.alibaba.graphscope.groot.coordinator.BackupInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClientBackupTest {

    String host = "localhost";
    int port = 55556;
    Client client = new Client(host, port);

    @Test
    public void testBackup() throws InterruptedException {
        Map<String, String> properties;
        for (int i = 0; i < 10; i++) {
            properties = new HashMap<>();
            properties.put("firstName", "test" + i);
            properties.put("id", "" + i);
            client.addVertex("person", properties);
        }
        client.commit();
        Thread.sleep(3000L);

        List<BackupInfo> backupInfoList;
        int backupId1 = client.createNewGraphBackup();
        Thread.sleep(3000L);

        backupInfoList = client.getGraphBackupInfo();
        Assertions.assertEquals(backupInfoList.size(), 1);
        Assertions.assertEquals(backupInfoList.get(0).getGlobalBackupId(), backupId1);

        for (int i = 10; i < 20; i++) {
            properties = new HashMap<>();
            properties.put("firstName", "test" + i);
            properties.put("id", "" + i);
            client.addVertex("person", properties);
        }
        client.commit();
        Thread.sleep(3000L);

        int backupId2 = client.createNewGraphBackup();
        Thread.sleep(3000L);

        backupInfoList = client.getGraphBackupInfo();
        Assertions.assertEquals(backupInfoList.size(), 2);
        Assertions.assertEquals(backupInfoList.get(0).getGlobalBackupId(), backupId1);
        Assertions.assertEquals(backupInfoList.get(1).getGlobalBackupId(), backupId2);

        Assertions.assertTrue(client.verifyGraphBackup(backupId1));
        Assertions.assertTrue(client.verifyGraphBackup(backupId2));

        client.restoreFromGraphBackup(backupId1, "./restored_meta_1", "./restored_data_1");
        client.restoreFromGraphBackup(backupId2, "./restored_meta_2", "./restored_data_2");

        client.purgeOldGraphBackups(1);
        backupInfoList = client.getGraphBackupInfo();
        Assertions.assertEquals(backupInfoList.size(), 1);
        Assertions.assertEquals(backupInfoList.get(0).getGlobalBackupId(), backupId2);

        client.deleteGraphBackup(backupId2);
        backupInfoList = client.getGraphBackupInfo();
        Assertions.assertTrue(backupInfoList.isEmpty());
    }
}

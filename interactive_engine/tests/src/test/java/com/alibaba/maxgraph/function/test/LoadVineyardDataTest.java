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
package com.alibaba.maxgraph.function.test;

import com.alibaba.maxgraph.function.test.config.Configuration;
import com.alibaba.maxgraph.function.test.config.ServiceConfig;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Test(groups = "load_data")
public class LoadVineyardDataTest {
    private Configuration testConf;
    // output
    private String objectId;
    private String schemaPath;
    private String testMode;

    @BeforeClass
    public void before() {
        testConf = TestGlobalMeta.getTestConf();
        this.testMode = ServiceConfig.FUNCTION_TEST_MODE.get(this.testConf);
    }

    @Test
    @Parameters("graphName")
    public void loadVineyardDataTest(String graphName) {
        if (this.testMode.equals("grape")) {
            return;
        }
        String loadCmd = ServiceConfig.LOAD_DATA_CMD(graphName).get(testConf);
        List<String> commandList = new ArrayList<>();
        if (ServiceConfig.LOAD_DATA_MODE.get(testConf).equals("k8s-exec")) {
            commandList.add("kubectl_exec.sh");
            commandList.add(ServiceConfig.ENGINE_NS.get(testConf));
            commandList.add(ServiceConfig.POD_HOSTS.get(testConf));
            commandList.add(ServiceConfig.CONTAINER_NAME.get(testConf));
            commandList.add(loadCmd);
        } else {
            Collections.addAll(commandList, loadCmd.split("\\s+"));
        }
        String[] commands = new String[commandList.size()];
        commands = commandList.toArray(commands);
        commands[0] = TestUtils.resourceAbsPath(commands[0]);

        // grant authority to execute
        TestUtils.runShellCmd(".", "chmod", "+x", commands[0]);
        Pair<Integer, String> res =
                TestUtils.runShellCmd(new File(commands[0]).getParent(), commands);

        // assert loading is successful
        Assert.assertEquals((int) res.getLeft(), 0);
        String[] msg = res.getValue().split("\\s+");

        objectId = msg[0];
        schemaPath = msg[1];
    }

    @AfterClass
    @Parameters("graphName")
    public void after(String graphName) {
        if (this.testMode.equals("grape")) {
            return;
        }
        TestGlobalMeta.addGraphMeta(graphName, Pair.of(this.objectId, this.schemaPath));
    }
}

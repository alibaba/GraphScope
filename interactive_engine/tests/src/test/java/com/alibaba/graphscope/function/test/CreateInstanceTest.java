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
package com.alibaba.graphscope.function.test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

import com.alibaba.graphscope.function.test.config.Configuration;
import com.alibaba.graphscope.function.test.config.ServiceConfig;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Test(groups = "create_instance")
public class CreateInstanceTest {
    public static final String ERROR_CODE = "errorCode";
    public static final String ERROR_MSG = "errorMessage";
    public static final String FRONTEND_PORT = "FRONTEND_PORT:";

    private Configuration testConf;
    // output
    private String gremlinEndpoint;
    private String testMode;

    @BeforeClass
    public void before() {
        this.testConf = TestGlobalMeta.getTestConf();
        this.testMode = ServiceConfig.FUNCTION_TEST_MODE.get(this.testConf);
    }

    @Test
    @Parameters("graphName")
    public void createInstanceTest(String graphName) throws Exception {
        if (this.testMode.equals("grape")) {
            loadCreateGraphByGrape(graphName);
            return;
        }
        String managerServer = ServiceConfig.MANAGER_SERVER_URL.get(testConf);
        String curlJson =
                TestUtils.curlHttp(
                        managerServer,
                        "createByPath",
                        createInstanceParameters(testConf, graphName));
        System.out.println(curlJson);

        // judge result
        Map<String, Object> res = TestUtils.getValuePairs(curlJson, ERROR_CODE, ERROR_MSG);
        String msg = (String) res.get(ERROR_MSG);
        Assert.assertTrue((int) res.get(ERROR_CODE) == 0 && msg.contains(FRONTEND_PORT));

        gremlinEndpoint = msg.split(FRONTEND_PORT)[1];
    }

    public static Map<String, String> createInstanceParameters(
            Configuration testConf, String graphName) {
        return new HashMap<String, String>() {
            {
                put("graphName", (String) TestGlobalMeta.getGraphMeta(graphName).getLeft());
                put("schemaPath", (String) TestGlobalMeta.getGraphMeta(graphName).getRight());
                put("podNameList", ServiceConfig.POD_HOSTS.get(testConf));
                put("containerName", ServiceConfig.CONTAINER_NAME.get(testConf));
                put("externalParams", ServiceConfig.ENGINE_PROPERTIES.get(testConf));
            }
        };
    }

    private void loadCreateGraphByGrape(String graphName1) {
        String loadCmd = ServiceConfig.LOAD_DATA_CMD(graphName1).get(testConf);
        String[] commands = new String[2];
        commands[0] = TestUtils.resourceAbsPath(loadCmd);
        commands[1] = ServiceConfig.CLIENT_SERVER_URL.get(testConf);
        TestUtils.runShellCmd(".", "chmod", "+x", commands[0]);
        Pair<Integer, String> res =
                TestUtils.runShellCmd(new File(commands[0]).getParent(), commands);
        Assert.assertEquals((int) res.getLeft(), 0);
        String[] msg = res.getRight().split("/");
        Assert.assertEquals(msg[0], "200");
        gremlinEndpoint = msg[3];
        System.out.println("gremlin is " + gremlinEndpoint);
    }

    @AfterClass
    @Parameters("graphName")
    public void after(String graphName) {
        // modern and classic share the same graph data
        TestGlobalMeta.addGraphEndpoint(graphName, gremlinEndpoint);
        if (graphName.equals(MODERN.name())) {
            TestGlobalMeta.addGraphEndpoint(CLASSIC.name(), gremlinEndpoint);
        }
    }
}

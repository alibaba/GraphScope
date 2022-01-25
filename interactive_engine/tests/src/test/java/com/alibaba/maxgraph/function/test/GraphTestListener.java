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

import com.alibaba.maxgraph.function.test.config.ConfigTemplateRender;
import com.alibaba.maxgraph.function.test.config.Configuration;
import com.alibaba.maxgraph.function.test.config.ServiceConfig;

import org.testng.ISuite;
import org.testng.ISuiteListener;

import java.util.HashMap;
import java.util.Map;

public class GraphTestListener implements ISuiteListener {
    public static final String FUNCTION_TEST_CONFIG = "function.test.config";

    @Override
    public void onStart(ISuite suite) {
        try {
            ConfigTemplateRender render =
                    new ConfigTemplateRender(TestUtils.getLoadlResource(FUNCTION_TEST_CONFIG));
            Configuration testConf = new Configuration(render.renderFromSysEnv());
            TestGlobalMeta.setTestConf(testConf);
        } catch (GraphTestException e) {
            System.err.println(e);
        }
    }

    @Override
    public void onFinish(ISuite iSuite) {
        try {
            if (!Boolean.valueOf(iSuite.getXmlSuite().getParameter("clean"))) return;
            Configuration testConf = TestGlobalMeta.getTestConf();
            String managerServer = ServiceConfig.MANAGER_SERVER_URL.get(testConf);
            for (String graphName : TestGlobalMeta.getAllGraphName()) {
                TestUtils.curlHttp(
                        managerServer, "close", deleteInstanceParameters(testConf, graphName));
            }
        } catch (GraphTestException e) {
            System.err.println(e);
        }
    }

    public static Map<String, String> deleteInstanceParameters(
            Configuration testConf, String graphName) {
        return new HashMap<String, String>() {
            {
                put("graphName", (String) TestGlobalMeta.getGraphMeta(graphName).getLeft());
                put("podNameList", ServiceConfig.POD_HOSTS.get(testConf));
                put("containerName", ServiceConfig.CONTAINER_NAME.get(testConf));
            }
        };
    }
}

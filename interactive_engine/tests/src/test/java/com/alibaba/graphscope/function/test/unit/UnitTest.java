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
package com.alibaba.graphscope.function.test.unit;

import com.alibaba.graphscope.function.test.TestUtils;

import org.junit.Test;

import java.util.Properties;

public class UnitTest {
    @Test
    public void propertiesTest() throws Exception {
        Properties properties = new Properties();
        System.out.println();
        properties.load(TestUtils.getLoadlResource("function.test.config"));
        String loadCmd = (String) properties.get("load.data.cmd");
        String[] commands = loadCmd.split("\\s+");
        // replace with absolute path
        commands[0] = TestUtils.resourceAbsPath(commands[0]);
        System.out.println(commands[0]);
    }
}

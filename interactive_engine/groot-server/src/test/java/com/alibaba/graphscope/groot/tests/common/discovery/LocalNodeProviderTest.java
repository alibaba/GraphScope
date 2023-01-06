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
package com.alibaba.graphscope.groot.tests.common.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.alibaba.graphscope.compiler.api.exception.GrootException;
import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.discovery.GrootNode;
import com.alibaba.graphscope.groot.discovery.LocalNodeProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LocalNodeProviderTest {

    @Test
    void testProvider() {
        RoleType role = RoleType.STORE;
        int idx = 2;
        int port = 1111;

        Configs configs =
                Configs.newBuilder()
                        .put("role.name", role.getName())
                        .put("node.idx", String.valueOf(idx))
                        .put("discovery.mode", "zookeeper")
                        .build();
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        GrootNode node = localNodeProvider.apply(port);

        Assertions.assertAll(
                () -> assertEquals(node.getRoleName(), role.getName()),
                () -> assertEquals(node.getIdx(), idx),
                () -> assertEquals(node.getPort(), port));

        assertThrows(GrootException.class, () -> localNodeProvider.apply(port));
    }
}

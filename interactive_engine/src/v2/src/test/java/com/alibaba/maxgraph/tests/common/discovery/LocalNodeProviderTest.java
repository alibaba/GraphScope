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
package com.alibaba.maxgraph.tests.common.discovery;

import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.discovery.LocalNodeProvider;
import com.alibaba.maxgraph.v2.common.discovery.MaxGraphNode;
import com.alibaba.maxgraph.v2.common.discovery.RoleType;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LocalNodeProviderTest {

    @Test
    void testProvider() {
        RoleType role = RoleType.STORE;
        int idx = 2;
        int port = 1111;

        Configs configs = Configs.newBuilder()
                .put("role.name", role.getName())
                .put("node.idx", String.valueOf(idx))
                .build();
        LocalNodeProvider localNodeProvider = new LocalNodeProvider(configs);
        MaxGraphNode node = localNodeProvider.apply(port);

        Assertions.assertAll(
                () -> assertEquals(node.getRoleName(), role.getName()),
                () -> assertEquals(node.getIdx(), idx),
                () -> assertEquals(node.getPort(), port)
        );

        assertThrows(MaxGraphException.class, () -> localNodeProvider.apply(port));
    }
}

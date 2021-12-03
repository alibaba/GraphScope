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
package com.alibaba.maxgraph.server;

import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class MaxGraphOpLoader {
    private static Map<String, OpProcessor> processors = Maps.newHashMap();

    public static Optional<OpProcessor> getProcessor(String name) {
        if (!processors.containsKey(name)) {
            return OpLoader.getProcessor(name);
        } else {
            return Optional.ofNullable(processors.get(name));
        }
    }

    public static Map<String, OpProcessor> getProcessors() {
        return Collections.unmodifiableMap(processors);
    }

    public static void addOpProcessor(String name, OpProcessor opProcessor) {
        processors.put(name, opProcessor);
    }
}

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
package com.alibaba.maxgraph.cache;

import com.alibaba.maxgraph.sdkcommon.graph.ElementId;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class CacheFactory {
    private static CacheFactory cacheFactory = new CacheFactory();

    private Cache<String, List<Object>> queryCache = CacheBuilder
            .newBuilder()
            .expireAfterAccess(10, TimeUnit.SECONDS)
            .maximumSize(100)
            .build();
    private Cache<ElementId, Vertex> vertexCache = CacheBuilder
            .newBuilder()
            .expireAfterAccess(30, TimeUnit.SECONDS)
            .maximumSize(100000)
            .build();

    private CacheFactory() {

    }

    public Cache<String, List<Object>> getQueryCache() {
        return queryCache;
    }

    public Cache<ElementId, Vertex> getVertexCache() {
        return vertexCache;
    }

    public static CacheFactory getCacheFactory() {
        return cacheFactory;
    }
}

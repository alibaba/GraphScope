/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.gaia.store;

import java.util.Set;

public interface GraphStoreService {
    long getLabelId(String label);

    long getGlobalId(long labelId, long propertyId);

    <P> P getVertexProperty(long id, String key);

    Set<String> getVertexKeys(long id);

    <P> P getEdgeProperty(long id, String key);

    Set<String> getEdgeKeys(long id);
}

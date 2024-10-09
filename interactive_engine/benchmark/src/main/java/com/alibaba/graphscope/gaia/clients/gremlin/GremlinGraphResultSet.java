/**
 * Copyright 2024 Alibaba Group Holding Limited.
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
package com.alibaba.graphscope.gaia.clients.gremlin;

import com.alibaba.graphscope.gaia.clients.GraphResultSet;

import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.Iterator;

public class GremlinGraphResultSet implements GraphResultSet {
    private final Iterator<Result> resultIterator;

    public GremlinGraphResultSet(ResultSet gremlinResultSet) {
        this.resultIterator = gremlinResultSet.iterator();
    }

    @Override
    public boolean hasNext() {
        return resultIterator.hasNext();
    }

    @Override
    public Object next() {
        return resultIterator.next();
    }
}

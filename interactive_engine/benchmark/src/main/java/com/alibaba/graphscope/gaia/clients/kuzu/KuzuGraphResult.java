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
package com.alibaba.graphscope.gaia.clients.kuzu;

import com.alibaba.graphscope.gaia.clients.GraphResultSet;
import com.kuzudb.KuzuFlatTuple;
import com.kuzudb.KuzuQueryResult;

public class KuzuGraphResult implements GraphResultSet {

    KuzuQueryResult result;

    public KuzuGraphResult(KuzuQueryResult result) {
        this.result = result;
    }

    public KuzuGraphResult() {
        this.result = null;
    }

    @Override
    public boolean hasNext() {
        if (result == null) {
            return false;
        }
        try {
            boolean hasNext = result.hasNext();
            if (!hasNext) {
                result.destroy();
            }
            return hasNext;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Object next() {
        if (result == null) {
            return null;
        }
        try {
            KuzuFlatTuple tuple = result.getNext();
            String tupleString = tuple.toString();
            tuple.destroy();
            return tupleString;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

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
package com.alibaba.maxgraph.iterator;

import com.alibaba.maxgraph.iterator.function.DefaultIteratorFunction;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class IteratorList<V, W> implements Iterator<W> {
    private List<Iterator<V>> iteratorList;
    private int iteratorIndex;
    private Function<V, W> function;
    private W value;

    public IteratorList(List<Iterator<V>> iteratorList) {
        this(iteratorList, new DefaultIteratorFunction<>());
    }

    public IteratorList(List<Iterator<V>> iteratorList, Function<V, W> function) {
        this.iteratorList = iteratorList;
        this.iteratorIndex = 0;
        this.function = function;
        this.value = null;
    }

    @Override
    public boolean hasNext() {
        while (iteratorIndex < iteratorList.size()) {
            Iterator<V> iterator = iteratorList.get(iteratorIndex);
            if (iterator.hasNext()) {
                value = null;
                return true;
            }
            iteratorIndex++;
        }

        return false;
    }

    @Override
    public W next() {
        if (null == value && iteratorIndex < iteratorList.size()) {
            value = function.apply(iteratorList.get(iteratorIndex).next());
        }

        return value;
    }
}

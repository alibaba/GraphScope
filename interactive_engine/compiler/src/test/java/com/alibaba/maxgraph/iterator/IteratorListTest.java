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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class IteratorListTest {
    @Test
    public void testIteratorList() {
        List<Iterator<Integer>> intItorList = Lists.newArrayList();
        intItorList.add(Lists.newArrayList(1, 2, 3).iterator());
        intItorList.add(Lists.newArrayList(3, 4, 5).iterator());
        intItorList.add(Lists.newArrayList(5, 6, 7).iterator());

        List<Integer> expectResult = Lists.newArrayList(1, 2, 3, 3, 4, 5, 5, 6, 7);
        List<Integer> result = Lists.newArrayList();
        IteratorList<Integer, Integer> iteratorList = new IteratorList<>(intItorList);
        while (iteratorList.hasNext()) {
            result.add(iteratorList.next());
        }
        Assert.assertEquals(expectResult, result);
    }
}

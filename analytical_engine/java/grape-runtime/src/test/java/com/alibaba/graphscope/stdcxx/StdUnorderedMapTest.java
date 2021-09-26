/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.stdcxx.StdUnorderedMap.Factory;
import com.alibaba.graphscope.utils.CppClassName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StdUnorderedMapTest {

    private Factory<Integer, Long> factory =
            FFITypeFactory.getFactory(
                    StdUnorderedMap.class, CppClassName.STD_UNORDERED_MAP + "<unsigned,uint64_t>");
    private StdUnorderedMap<Integer, Long> map;

    @Before
    public void init() {
        map = factory.create();
    }

    @Test
    public void test1() {
        for (int i = 0; i < 10; ++i) {
            map.set(i, (long) i);
        }
        for (int i = 0; i < 10; ++i) {
            Assert.assertTrue("Different at " + i, i == map.get(i).intValue());
        }
    }
}

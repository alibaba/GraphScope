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

package com.alibaba.graphscope.ds;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BitsetTest {

    private Bitset bitset;

    @Before
    public void before() {
        bitset = Bitset.factory.create();
        bitset.init(20);
    }

    @Test
    public void test1() {
        for (int i = 0; i < 10; i += 2) {
            bitset.setBit((long) i);
        }
        for (int i = 0; i < 10; i += 2) {
            Assert.assertTrue(bitset.getBit((long) i));
        }
        for (int i = 1; i < 10; i += 2) {
            Assert.assertFalse(bitset.getBit((long) i));
        }
    }
}

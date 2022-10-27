/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.utils;

import org.junit.Assert;
import org.junit.Test;

public class ThreadSafeBitSetTest {

    @Test
    public void test1() {
        ThreadSafeBitSet set =
                new ThreadSafeBitSet(ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, 16);
        set.setUntil(10);
        for (int i = 0; i < 10; ++i) {
            Assert.assertTrue(set.get(i));
        }
        for (int i = 10; i < 16; ++i) {
            Assert.assertFalse(set.get(i));
        }
    }

    @Test
    public void test2() {
        ThreadSafeBitSet set =
                new ThreadSafeBitSet(ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, 16);
        set.setUntil(16);
        for (int i = 0; i < 16; ++i) {
            Assert.assertTrue(set.get(i));
        }
    }

    @Test
    public void test3() {
        ThreadSafeBitSet set =
                new ThreadSafeBitSet(ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, 100);
        set.setUntil(65);
        for (int i = 0; i < 65; ++i) {
            Assert.assertTrue(set.get(i));
        }
        for (int i = 65; i < 100; ++i) {
            Assert.assertFalse(set.get(i));
        }
    }

    @Test
    public void test4() {
        ThreadSafeBitSet set =
                new ThreadSafeBitSet(ThreadSafeBitSet.DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS, 100);
        set.setUntil(33);
        for (int i = 0; i < 33; ++i) {
            Assert.assertTrue(set.get(i));
        }
        for (int i = 33; i < 100; ++i) {
            Assert.assertFalse(set.get(i));
        }
    }
}

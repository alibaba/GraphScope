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

import org.junit.Test;

public class WildCardTest {

    public static class Conf<A, B, C> {

        private A a;

        public Conf() {}

        public void setA(A a) {
            this.a = a;
        }

        public A getA() {
            return a;
        }
    }

    public static <A, B, C> Conf<A, B, C> create(
            Class<? super A> clz, Class<? super B> clz2, Class<? super C> clz3) {
        return new Conf<A, B, C>();
    }

    @Test
    public void test1() {
        Conf<Long, Double, Integer> conf = create(Long.class, Double.class, Integer.class);
    }
}

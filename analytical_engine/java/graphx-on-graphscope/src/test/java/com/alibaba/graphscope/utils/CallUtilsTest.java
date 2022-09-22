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

public class CallUtilsTest {
    public static class A {
        public static void a() {
            System.out.println(CallUtils.getCallerCallerClassName());
        }
    }

    public static class B {
        public static void b() {
            A.a();
        }
    }

    public static class C {
        public static void c() {
            B.b();
        }
    }

    @Test
    public void test() {
        C.c();
    }
}

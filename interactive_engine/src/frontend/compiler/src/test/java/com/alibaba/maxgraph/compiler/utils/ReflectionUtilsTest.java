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
package com.alibaba.maxgraph.compiler.utils;

import org.junit.Assert;
import org.junit.Test;

public class ReflectionUtilsTest {

    @Test
    public void testGetFieldValue() {
        PrivateValueClass valueClass = new PrivateValueClass(123, "hello");

        int intValue = ReflectionUtils.getFieldValue(PrivateValueClass.class, valueClass, "intValue");
        Assert.assertEquals(123, intValue);

        String strValue = ReflectionUtils.getFieldValue(PrivateValueClass.class, valueClass, "strValue");
        Assert.assertEquals("hello", strValue);
    }

    private static class PrivateValueClass {
        private int intValue;
        private String strValue;

        public PrivateValueClass(int intValue, String strValue) {
            this.intValue = intValue;
            this.strValue = strValue;
        }
    }
}

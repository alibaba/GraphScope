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
package com.alibaba.maxgraph.tests.frontend.utils;

import com.alibaba.maxgraph.v2.frontend.utils.ReflectionUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReflectionUtilTest {
    @Test
    void testReflectionField() {
        ReflectionObject object = new ReflectionObject();
        ReflectionUtil.setFieldValue(ReflectionObject.class, object, "value", "hello world");
        assertEquals("hello world", ReflectionUtil.getFieldValue(ReflectionObject.class, object, "value"));

        assertThrows(RuntimeException.class, () -> ReflectionUtil.setFieldValue(ReflectionObject.class, object, "value", 123));
        assertThrows(RuntimeException.class, () -> ReflectionUtil.getFieldValue(ReflectionObject.class, object, "value0"));
    }
}

class ReflectionObject {
    private String value;
}

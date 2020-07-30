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
package com.alibaba.maxgraph.sdkcommon.util;

import com.alibaba.maxgraph.sdkcommon.util.JSON;
import lombok.Getter;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by wubincen on 2018/5/4
 */
public class JSONTest {

    static class TestObject {
        @Getter private String stringVal1;
        @Getter private int intVal1;
        @Getter private double doubleVal1;

        public TestObject() {
        }

        public TestObject(String stringVal1, int intVal1, double doubleVal1) {
            this.stringVal1 = stringVal1;
            this.intVal1 = intVal1;
            this.doubleVal1 = doubleVal1;
        }

        @Override
        public boolean equals(Object obj) {
            TestObject other = (TestObject)obj;
            return this.stringVal1.equals(other.stringVal1) &&
                this.doubleVal1 == other.doubleVal1 &&
                this.intVal1 == other.intVal1;
        }
    }

    @Test
    public void testNormalObject() {
        TestObject object = new TestObject("aaaaa", 111, 22.22);
        TestObject object1 = JSON.fromJson(JSON.toJson(object), TestObject.class);
        Assert.assertEquals(object, object1);
    }

    static enum TestEnum {
        A,
        B,
        C;
    }

    static class TestEnumObject {
        @Getter private TestEnum enumVal;

        public TestEnumObject() {
        }

        public TestEnumObject(TestEnum enumVal) {
            this.enumVal = enumVal;
        }

        @Override
        public boolean equals(Object obj) {
            TestEnumObject other = (TestEnumObject)obj;
            return this.enumVal == other.enumVal;
        }
    }

    @Test
    public void testEnum() {
        TestEnumObject object = new TestEnumObject(TestEnum.valueOf("A"));
        TestEnumObject object1 = JSON.fromJson(JSON.toJson(object), TestEnumObject.class);
        Assert.assertEquals(object, object1);
    }
}

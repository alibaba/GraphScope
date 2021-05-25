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
package com.alibaba.maxgraph.tinkerpop;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import com.google.common.base.Preconditions;

/**
 * @author beimian
 */
public final class Utils {

    private Utils() {};

    public static Map<String, Object> convertToMap(final Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new RuntimeException("keyValues length is " + keyValues.length + ", and can't convert.");
        }

        Map<String, Object> kv = new HashMap<>(keyValues.length >> 1);
        for (int i = 0; i < keyValues.length; i+=2) {
            kv.put(keyValues[i].toString(), keyValues[i + 1]);
        }

        return kv;
    }

    public static Map<Object, Object> convertToRawMap(final Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new RuntimeException("keyValues length is " + keyValues.length + ", and can't convert.");
        }

        Map<Object, Object> kv = new HashMap<>(keyValues.length >> 1);
        for (int i = 0; i < keyValues.length; i+=2) {
            kv.put(keyValues[i], keyValues[i + 1]);
        }

        return kv;
    }

    /**
     * Set private field with given value
     *
     * @param obj       The given instance
     * @param fieldName The field name
     * @param value     The field value
     * @param <V>       The field value type
     */
    public static <V> void setFieldValue(Class<?> clazz, Object obj, String fieldName, V value) {
        Preconditions.checkNotNull(obj);
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(value);

        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(obj, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get private field from given instance
     *
     * @param obj       The given instance
     * @param fieldName The field name
     * @return The field value
     */
    public static <V> V getFieldValue(Class<?> clazz, Object obj, String fieldName) {
        Preconditions.checkNotNull(obj);
        Preconditions.checkNotNull(fieldName);

        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (V) field.get(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

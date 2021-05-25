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
package com.alibaba.maxgraph.v2.frontend.utils;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;

/**
 * Reflection related util, get and set private field in the given class and object
 */
public class ReflectionUtil {
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

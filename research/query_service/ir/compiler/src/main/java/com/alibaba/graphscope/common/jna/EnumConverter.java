/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.jna;

import com.sun.jna.FromNativeContext;
import com.sun.jna.ToNativeContext;
import com.sun.jna.TypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnumConverter implements TypeConverter {
    private static final Logger logger = LoggerFactory.getLogger(EnumConverter.class);

    public Object fromNative(Object input, FromNativeContext context) {
        Integer i = (Integer) input;
        Class targetClass = context.getTargetType();
        if (!IntEnum.class.isAssignableFrom(targetClass)) {
            return null;
        }
        Object[] enums = targetClass.getEnumConstants();
        if (enums.length == 0) {
            logger.error(
                    "Could not convert desired enum type (), no valid values are defined.",
                    targetClass.getName());
            return null;
        }
        IntEnum instance = (IntEnum) enums[0];
        return instance.getEnum(i);
    }

    public Object toNative(Object input, ToNativeContext context) {
        if (input == null) {
            return new Integer(0);
        }
        IntEnum j = (IntEnum) input;
        return new Integer(j.getInt());
    }

    public Class nativeType() {
        return Integer.class;
    }
}

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

package com.alibaba.graphscope.dataload.jna.helper;

import com.sun.jna.FunctionMapper;
import com.sun.jna.NativeLibrary;

import java.lang.reflect.Method;

public class IrFunctionMapper implements FunctionMapper {
    public static IrFunctionMapper INSTANCE = new IrFunctionMapper();

    private IrFunctionMapper() {
        super();
    }

    @Override
    public String getFunctionName(NativeLibrary nativeLibrary, Method method) {
        String target = method.getName();
        String[] splits = target.split("(?=\\p{Lu})");
        StringBuilder cName = new StringBuilder();
        for (int i = 0; i < splits.length; ++i) {
            if (i != 0) {
                cName.append("_");
            }
            cName.append(splits[i].toLowerCase());
        }
        return cName.toString();
    }
}

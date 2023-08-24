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

package com.alibaba.graphscope.common.config;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class Utils {
    // convert from 'a,b,c' to list type: [a, b, c]
    public static List<String> convertDotString(String dotString) {
        String[] stringArray = dotString.split(",");
        List<String> stringList = Lists.newArrayList();
        if (stringArray != null && stringArray.length > 0) {
            for (String str : stringArray) {
                String trim = str.trim();
                if (!StringUtils.isEmpty(trim)) {
                    stringList.add(trim);
                }
            }
        }
        return stringList;
    }
}

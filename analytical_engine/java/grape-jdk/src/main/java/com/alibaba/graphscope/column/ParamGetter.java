/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.column;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class ParamGetter {
    private JSONObject jsonObject;

    public ParamGetter() {}

    public void init(String str) {
        jsonObject = JSON.parseObject(str);
    }

    public String get(String str) {
        return jsonObject.getString(str);
    }

    public String getOrDefault(String str, String defaultValue) {
        String res = jsonObject.getString(str);
        if (res == null) return defaultValue;
        return res;
    }

    public Long getLong(String str) {
        return jsonObject.getLong(str);
    }

    public Double getDouble(String str) {
        return jsonObject.getDouble(str);
    }

    public Integer getInteger(String str) {
        return jsonObject.getInteger(str);
    }
}

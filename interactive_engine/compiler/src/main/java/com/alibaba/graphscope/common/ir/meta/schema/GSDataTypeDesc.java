/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.schema;

import com.alibaba.graphscope.proto.type.Common;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;

import java.util.Map;

public class GSDataTypeDesc {
    // support more format of GSDataTypeDesc, i.e. JSON, proto, etc.
    private final Map<String, Object> yamlDesc;

    // flex type in proto format
    private Common.DataType protoDesc;

    public GSDataTypeDesc(Map<String, Object> yamlDesc) {
        this.yamlDesc = yamlDesc;
    }

    public Map<String, Object> getYamlDesc() {
        return yamlDesc;
    }

    public Common.DataType getProtoDesc() throws Exception {
        if (protoDesc != null) return protoDesc;
        Common.DataType.Builder protoBuilder = Common.DataType.newBuilder();
        String jsonDesc = new ObjectMapper().writeValueAsString(yamlDesc);
        JsonFormat.parser().merge(jsonDesc, protoBuilder);
        protoDesc = protoBuilder.build();
        return protoDesc;
    }

    public String toString() {
        return yamlDesc.toString();
    }
}

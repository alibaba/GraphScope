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

package com.alibaba.graphscope.sdk;

import com.alibaba.graphscope.proto.frontend.Code;

import java.io.Serializable;

public class GraphPlan implements Serializable {
    public String errorCode;
    public String fullMessage;
    public final byte[] physicalBytes;
    public String resultSchemaYaml;

    public GraphPlan(
            Code errorCode, String fullMessage, byte[] physicalBytes, String resultSchemaYaml) {
        this.errorCode = analyzeError(errorCode, physicalBytes).name();
        this.fullMessage = fullMessage;
        this.physicalBytes = physicalBytes;
        this.resultSchemaYaml = resultSchemaYaml;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getFullMessage() {
        return fullMessage;
    }

    public byte[] getPhysicalBytes() {
        return physicalBytes;
    }

    public String getResultSchemaYaml() {
        return resultSchemaYaml;
    }

    private Enum<?> analyzeError(Code errorCode, byte[] physicalBytes) {
        switch (errorCode) {
            case OK:
                if (physicalBytes == null) return ExtendCode.EMPTY_RESULT;
            default:
                return errorCode;
        }
    }

    enum ExtendCode {
        EMPTY_RESULT
    }
}

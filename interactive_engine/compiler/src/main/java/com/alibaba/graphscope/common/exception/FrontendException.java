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

package com.alibaba.graphscope.common.exception;

import com.alibaba.graphscope.proto.frontend.Code;
import com.google.common.collect.Maps;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

public class FrontendException extends RuntimeException {
    private final ComponentCode componentCode;
    private final Code errorCode;
    private final Map<String, Object> details;

    public FrontendException(Code errorCode, String errorMsg) {
        this(errorCode, errorMsg, null);
    }

    public FrontendException(Code errorCode, String errorMsg, @Nullable Throwable cause) {
        this(errorCode, errorMsg, Maps.newHashMap(), cause);
    }

    public FrontendException(
            Code errorCode,
            String errorMsg,
            Map<String, Object> details,
            @Nullable Throwable cause) {
        super(errorMsg);
        if (cause != null) {
            setStackTrace(cause.getStackTrace());
        }
        this.componentCode = ComponentCode.FRONTEND;
        this.errorCode = errorCode;
        this.details = Maps.newHashMap(details);
    }

    public Map getDetails() {
        return this.details;
    }

    public Code getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("ErrorCode: ").append(errorCode.name()).append("\n");
        String msg = super.getMessage();
        if (!msg.endsWith("\n")) {
            msg += "\n";
        }
        sb.append("Message: ").append(msg);
        sb.append("EC: ")
                .append(String.format("%02d-%04d", componentCode.getValue(), errorCode.getNumber()))
                .append("\n");
        details.forEach(
                (k, v) -> {
                    sb.append(k).append(": ").append(v).append("\n");
                });
        return sb.toString();
    }
}

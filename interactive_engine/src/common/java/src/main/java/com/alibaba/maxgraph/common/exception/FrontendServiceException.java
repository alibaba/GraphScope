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
package com.alibaba.maxgraph.common.exception;

import com.alibaba.maxgraph.sdkcommon.util.ExceptionUtils.ErrorCode;

import java.text.MessageFormat;

public class FrontendServiceException extends Exception {
    private int errorCode;
    private String msg;

    public FrontendServiceException(ErrorCode code, String msg) {
        super(msg);
        this.msg = msg;
        this.errorCode = code.toInt();
    }

    public FrontendServiceException(String msg) {
        this(ErrorCode.Unknown, msg);
    }

    public static FrontendServiceException illegalSession(String sessionId) {
        String msg = MessageFormat.format("the session {0} is illegal", sessionId);
        return new FrontendServiceException(ErrorCode.IllegalSession, msg);
    }

    public static FrontendServiceException sessionTimeout(String sessionId) {
        String msg = MessageFormat.format("the session {0} is timeout", sessionId);
        return new FrontendServiceException(ErrorCode.SessionTimeout, msg);
    }

    public static FrontendServiceException frontendServiceBusy(String frontendIp) {
        String msg = MessageFormat.format("the frontend service {0} is busy", frontendIp);
        return new FrontendServiceException(ErrorCode.FrontendServiceBusy, msg);
    }

    public static FrontendServiceException realtimeWriteFailed() {
        return new FrontendServiceException(ErrorCode.FrontendServiceBusy, "write data failed");
    }

    public static FrontendServiceException serviceNotReady() {
        return new FrontendServiceException(ErrorCode.ServiceNotReady, "service not ready");
    }

    public static FrontendServiceException dataNotValid(String data) {
        String msg = MessageFormat.format("the data {0} is inconsistency with schema", data);
        return new FrontendServiceException(ErrorCode.ServiceNotReady, msg);
    }
}

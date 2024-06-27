/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.graphscope.interactive.client.common;

import com.alibaba.graphscope.interactive.ApiException;
import com.alibaba.graphscope.interactive.ApiResponse;

/**
 * Mapping http status code to our status code, along with a message
 */
public class Status {
    enum StatusCode {
        kOk, // 200
        kBadRequest, // 400
        kForbidden, // 403
        kNotFound, // 404
        kServerInternalError, // 500
        kServiceUnavailable, // 503
        kUnknown, // default
    }

    private final StatusCode code;
    private final String message;

    public Status() {
        this.code = StatusCode.kUnknown;
        this.message = "";
    }

    public boolean IsOk() {
        return this.code == StatusCode.kOk;
    }

    public String getMessage() {
        return message;
    }

    public Status(StatusCode code, String message) {
        this.code = code;
        this.message = message;
    }

    public static Status ok(String message) {
        return new Status(StatusCode.kOk, message);
    }

    public static Status badRequest(String message) {
        return new Status(StatusCode.kBadRequest, message);
    }

    public static Status serverInternalError(String message) {
        return new Status(StatusCode.kServerInternalError, message);
    }

    public static Status fromException(ApiException exception) {
        // mapping exception's http code to our status code
        switch (exception.getCode()) {
            case 400:
                return new Status(StatusCode.kBadRequest, exception.getMessage());
            case 403:
                return new Status(StatusCode.kForbidden, exception.getMessage());
            case 404:
                return new Status(StatusCode.kNotFound, exception.getMessage());
            case 500:
                return new Status(StatusCode.kServerInternalError, exception.getMessage());
            case 503:
                return new Status(StatusCode.kServiceUnavailable, exception.getMessage());
            default:
                return new Status(StatusCode.kUnknown, exception.getMessage());
        }
    }

    public static <T> Status fromResponse(ApiResponse<T> response) {
        // mapping response's http code to our status code
        switch (response.getStatusCode()) {
            case 200:
                return new Status(StatusCode.kOk, "");
            case 400:
                return new Status(StatusCode.kBadRequest, "");
            case 403:
                return new Status(StatusCode.kForbidden, "");
            case 404:
                return new Status(StatusCode.kNotFound, "");
            case 500:
                return new Status(StatusCode.kServerInternalError, "");
            case 503:
                return new Status(StatusCode.kServiceUnavailable, "");
            default:
                return new Status(StatusCode.kUnknown, "");
        }
    }

    public static Status Ok() {
        return new Status(StatusCode.kOk, "");
    }
}

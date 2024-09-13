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
import com.alibaba.graphscope.interactive.proto.Code;

/**
 * Mapping http status code to our status code, along with a message
 */
public class Status {

    private final Code code;
    private final String message;

    public Status() {
        this.code = Code.OK;
        this.message = "";
    }

    public boolean IsOk() {
        return this.code == Code.OK;
    }

    public String getMessage() {
        return message;
    }

    public Code getCode() {
        return this.code;
    }

    public Status(Code code, String message) {
        this.code = code;
        this.message = message;
    }

    public static Status ok(String message) {
        return new Status(Code.OK, message);
    }

    public static Status badRequest(String message) {
        return new Status(Code.BAD_REQUEST, message);
    }

    public static Status serverInternalError(String message) {
        return new Status(Code.INTERNAL_ERROR, message);
    }

    public static Status fromException(ApiException exception) {
        // mapping exception's http code to our status code
        switch (exception.getCode()) {
            case 400:
                return new Status(Code.BAD_REQUEST, exception.getMessage());
            case 403:
                return new Status(Code.PERMISSION_DENIED, exception.getMessage());
            case 404:
                return new Status(Code.NOT_FOUND, exception.getMessage());
            case 503:
                return new Status(Code.SERVICE_UNAVAILABLE, exception.getMessage());
            default:
                return new Status(Code.INTERNAL_ERROR, exception.getMessage());
        }
    }

    public static <T> Status fromResponse(ApiResponse<T> response) {
        // mapping response's http code to our status code
        switch (response.getStatusCode()) {
            case 200:
                return new Status(Code.OK, "OK");
            case 400:
                return new Status(Code.BAD_REQUEST, "Bad request");
            case 403:
                return new Status(Code.PERMISSION_DENIED, "Permission denied");
            case 404:
                return new Status(Code.NOT_FOUND, "Not found");
            case 503:
                return new Status(Code.SERVICE_UNAVAILABLE, "Service unavailable");
            default:
                return new Status(Code.INTERNAL_ERROR, "Internal error");
        }
    }

    public static Status Ok() {
        return new Status(Code.OK, "");
    }
}

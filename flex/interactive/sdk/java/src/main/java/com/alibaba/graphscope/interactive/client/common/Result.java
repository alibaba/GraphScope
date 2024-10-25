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

/***
 * A class which wrap the result of the API
 */
public class Result<T> {
    private final Status status;
    private final T value;

    public Result(Status status) {
        this.status = status;
        this.value = null;
    }

    public Result(Status status, T value) {
        this.status = status;
        this.value = value;
    }

    public Result(T value) {
        this.status = Status.Ok();
        this.value = value;
    }

    public Status getStatus() {
        return status;
    }

    public String getStatusMessage() {
        return status.getMessage();
    }

    public Code getStatusCode() {
        return status.getCode();
    }

    public T getValue() {
        return value;
    }

    public static <T> Result<T> ok(T value) {
        return new Result<T>(Status.Ok(), value);
    }

    public static <T> Result<T> error(String message) {
        return new Result<T>(new Status(Code.UNKNOWN, message), null);
    }

    public static <T> Result<T> fromException(ApiException exception) {
        return new Result<T>(Status.fromException(exception), null);
    }

    public static <T> Result<T> fromResponse(ApiResponse<T> response) {
        return new Result<T>(Status.fromResponse(response), response.getData());
    }

    public boolean isOk() {
        return status.IsOk();
    }
}

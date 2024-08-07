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
package com.alibaba.graphscope.groot.common.exception;

import com.alibaba.graphscope.proto.Code;

public class GrootException extends RuntimeException {
    private final int componentCode = 6;

    private int errorCode;

    public GrootException(Code code) {
        super();
        this.errorCode = code.getNumber();
    }

    public GrootException(Code code, Throwable t) {
        super(t);
        this.errorCode = code.getNumber();
    }

    public GrootException(Code code, String msg) {
        super(msg);
        this.errorCode = code.getNumber();
    }

    public GrootException(Code code, String msg, Throwable t) {
        super(msg, t);
        this.errorCode = code.getNumber();
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public GrootException(Throwable t) {
        super(t);
    }

    public GrootException(String msg) {
        super(msg);
    }

    public GrootException(String msg, Throwable t) {
        super(msg, t);
    }

    public GrootException() {
        super();
    }

    @Override
    public String getMessage() {
        String ec = String.format("%02d-%04d", componentCode, errorCode);
        return String.format("ec=%s, %s", ec, super.getMessage());
    }
}

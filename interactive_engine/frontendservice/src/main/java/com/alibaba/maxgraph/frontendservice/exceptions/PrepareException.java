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
package com.alibaba.maxgraph.frontendservice.exceptions;

import com.google.common.base.Preconditions;

public class PrepareException extends Exception {
    public enum ErrorCode {
        Ok,
        alreadyPrepared,
        unRecognizedPrepare,
        persistPrepareError,
        fetchLockFailed,
        errorToPrepare,
        readMetaError,
        unknown;

        public int value() {
            return this.ordinal();
        }
    }

    private ErrorCode errorCode;
    private Object message;


    public PrepareException(int error) {
        super();
        Preconditions.checkArgument(error != 0);
        this.errorCode = ErrorCode.values()[error];
    }

    public PrepareException(int error, Object message) {
        super(message.toString());
        Preconditions.checkArgument(error != 0);
        if(error < 0) {
            this.errorCode = ErrorCode.unknown;
        } else {
            this.errorCode = ErrorCode.values()[error];
        }
        this.message = message;
    }


    public PrepareException(ErrorCode error, Object message) {
        super(message.toString());
        Preconditions.checkArgument(error.ordinal() != 0);
        this.errorCode = error;
        this.message = message;
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    public Object getErrorMessage() {
        return this.message;
    }

    @Override
    public String toString() {
        return this.errorCode.name() + ": " + this.message;
    }

    public static PrepareException parseFrom(int code, String message) {
        return new PrepareException(code, message);
    }
}

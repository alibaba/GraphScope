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
package com.alibaba.maxgraph.compiler.exception;

import java.text.MessageFormat;

public class DataException extends RuntimeException {

    public enum ErrorCode {

        TypeNotFound("{0} type not found.", 100),
        PKIncomplete("PK of {0} is in complete", 101),
        UNKNOWN("Unknown error. ", 200),
        InvalidProperty("Property : {0} is invalid, because {1} ", 201);

        private String template;
        public final int code;

        ErrorCode(String template, int code) {
            this.template = template;
            this.code = code;
        }

        public String toString(String ...param) {
            if (param != null) {
                return MessageFormat.format(template, param);
            } else {
                return template;
            }
        }
    }

    public final int errorCode;

    public DataException(ErrorCode code, Throwable cause) {
        super(code.toString(null), cause);
        this.errorCode = code.code;
    }

    public DataException(ErrorCode code, Throwable cause, String ...param) {
        super(code.toString(param), cause);
        this.errorCode = code.code;
    }

    public static DataException typeNotFound(String label) {
        return new DataException(ErrorCode.TypeNotFound, null, label);
    }

    public static DataException pkIncomplete(String label) {
        return new DataException(ErrorCode.PKIncomplete, null, label);
    }

    public static DataException invalidProperty(String propertyName, String cause) {
        return new DataException(ErrorCode.InvalidProperty, null, propertyName, cause);
    }
    public static DataException unknowError(Throwable e) {
        return new DataException(ErrorCode.UNKNOWN, e);
    }
}

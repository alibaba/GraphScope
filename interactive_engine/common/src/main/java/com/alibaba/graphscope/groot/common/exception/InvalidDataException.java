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

public class InvalidDataException extends GrootException {

    public InvalidDataException(String msg) {
        super(Code.INVALID_DATA, msg);
    }

    public InvalidDataException(String msg, Throwable t) {
        super(Code.INVALID_DATA, msg, t);
    }

    public InvalidDataException() {
        super(Code.INVALID_DATA);
    }

    public InvalidDataException(Throwable t) {
        super(Code.INVALID_DATA, t);
    }
}

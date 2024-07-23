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

public class PropertyNotFoundException extends GrootException {
    public PropertyNotFoundException() {
        super(Code.PROPERTY_NOT_FOUND);
    }

    public PropertyNotFoundException(Exception exception) {
        super(Code.PROPERTY_NOT_FOUND, exception);
    }

    public PropertyNotFoundException(String message) {
        super(Code.PROPERTY_NOT_FOUND, message);
    }

    public PropertyNotFoundException(String message, Exception exception) {
        super(Code.PROPERTY_NOT_FOUND, message, exception);
    }
}

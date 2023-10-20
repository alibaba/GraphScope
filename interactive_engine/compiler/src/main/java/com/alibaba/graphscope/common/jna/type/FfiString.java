/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.sun.jna.Pointer;
import com.sun.jna.PointerType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FfiString extends PointerType {
    private static final Logger logger = LoggerFactory.getLogger(FfiString.class);
    private final String value;

    public FfiString(Pointer cstr) {
        super(cstr);
        this.value = (cstr == null) ? StringUtils.EMPTY : cstr.getString(0);
        release();
    }

    public String getValue() {
        return this.value;
    }

    private void release() {
        Pointer cstr = getPointer();
        if (cstr != null) {
            logger.info("free ffi str pointed by {}", cstr);
            IrCoreLibrary.INSTANCE.destroyCstrPointer(cstr);
        }
    }
}

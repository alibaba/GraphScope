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

import com.alibaba.graphscope.common.jna.IntEnum;

public enum ResultOpt implements IntEnum<ResultOpt> {
    EndV,
    AllV,
    AllVe;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public ResultOpt getEnum(int i) {
        ResultOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}

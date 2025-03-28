/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.rel.type;

/**
 * Define the data format of the external source, which adheres to the {@code Format} in data loading specification.
 */
public class DataFormat {
    private String delimiter = ",";
    private boolean hasHeader = false;
    // todo(yihe.zxl): support other aspects of data format, i.e. quoting, escape_char, etc.

    public DataFormat withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public DataFormat withHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
        return this;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public boolean hasHeader() {
        return hasHeader;
    }
}

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

package com.alibaba.graphscope.common.intermediate;

public class ArgVariable {
    private final String tagName;
    private final String propertyName;

    public ArgVariable(String tagName, String propertyName) {
        this.tagName = tagName;
        this.propertyName = propertyName;
    }

    @Override
    public String toString() {
        return (propertyName == null || propertyName.isEmpty())
                ? "@" + tagName
                : "@" + tagName + "." + propertyName;
    }
}

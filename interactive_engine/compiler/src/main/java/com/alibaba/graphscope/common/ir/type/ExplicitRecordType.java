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

package com.alibaba.graphscope.common.ir.type;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

import java.util.Objects;

/**
 * Always return the explicit type for each field
 */
public class ExplicitRecordType extends RelRecordType {
    private final RelDataType explicitType;

    public ExplicitRecordType(RelDataType explicitType) {
        super(
                StructKind.FULLY_QUALIFIED,
                ImmutableList.of(new RelDataTypeFieldImpl("", 0, explicitType)),
                false);
        this.explicitType = Objects.requireNonNull(explicitType);
    }

    public RelDataType getExplicitType() {
        return explicitType;
    }
}

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

package com.alibaba.graphscope.common.calcite.util;

import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.NOT_EQUALS;

import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlKind;

import java.util.EnumSet;
import java.util.Set;

/**
 * Definitions of objects to be statically imported
 */
public abstract class Static {
    private Static() {}

    /** Resources. */
    public static final GraphResource RESOURCE = Resources.create(GraphResource.class);

    public static final Set<SqlKind> BINARY_COMPARISON =
            EnumSet.of(
                    EQUALS,
                    NOT_EQUALS,
                    GREATER_THAN,
                    GREATER_THAN_OR_EQUAL,
                    LESS_THAN,
                    LESS_THAN_OR_EQUAL);

    public static final String DELIMITER = ".";
}

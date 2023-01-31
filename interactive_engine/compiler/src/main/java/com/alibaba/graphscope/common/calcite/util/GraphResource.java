/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://github.com/apache/calcite/blob/main/core/src/main/java/org/apache/calcite/runtime/CalciteResource.java
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.calcite.util;

import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.CalciteResource;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.runtime.Resources.ExInst;

public interface GraphResource extends CalciteResource {
    @Resources.BaseMessage("Type of ''{0}'' should be ''{1}'' but is ''{2}''")
    ExInst<CalciteException> incompatibleTypes(String var0, Class<?> var1, Class<?> var2);

    /**
     * we will never implement the inherited function in this subclass for it will never be used
     * @param var0
     * @return
     */
    @Resources.BaseMessage("Function is inherited and will not be implemented in subclass ''{0}''")
    ExInst<CalciteException> functionNotImplement(Class<?> var0);

    /**
     * we will implement the inherited function in this subclass in the near future, but it is not used currently
     * @param var0
     * @return
     */
    @Resources.BaseMessage("Function is inherited and will be implemented in subclass ''{0}''")
    ExInst<CalciteException> functionWillImplement(Class<?> var0);

    @Resources.BaseMessage("Cannot apply ''{0}'' to operands of type {1}. Supported form(s): {2}")
    ExInst<CalciteException> canNotApplyOpToOperands(String var0, String var1, String var2);

    @Resources.BaseMessage("Illegal use of ''NULL''")
    ExInst<CalciteException> illegalNull();
}

/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.app;

import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.ParallelPropertyMessageManager;

/**
 * Base interface for all parallel property app.
 *
 * @param <OID_T> original id type.
 * @param <C> context type, we use default context base.
 */
public interface ParallelPropertyAppBase<OID_T, C extends PropertyParallelContextBase<OID_T>> {
    void PEval(
            ArrowFragment<OID_T> fragment,
            PropertyParallelContextBase<OID_T> context,
            ParallelPropertyMessageManager messageManager);

    void IncEval(
            ArrowFragment<OID_T> graph,
            PropertyParallelContextBase<OID_T> context,
            ParallelPropertyMessageManager messageManager);
}

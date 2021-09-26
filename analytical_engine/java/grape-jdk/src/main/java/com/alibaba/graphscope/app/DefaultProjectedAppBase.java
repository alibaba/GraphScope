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

import com.alibaba.graphscope.context.ProjectedDefaultContextBase;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;

public interface DefaultProjectedAppBase<
        OID_T,
        VID_T,
        VDATA_T,
        EDATA_T,
        C extends
                ProjectedDefaultContextBase<
                                ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>>> {
    void PEval(
            ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> fragment,
            ProjectedDefaultContextBase<ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>>
                    context,
            DefaultMessageManager messageManager);

    void IncEval(
            ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph,
            ProjectedDefaultContextBase<ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>>
                    context,
            DefaultMessageManager messageManager);
}

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

import com.alibaba.graphscope.context.ContextBase;

/**
 * Root Interface for DefaultAppBase and ParallelAppBase.
 *
 * @param <OID_T> original id type
 * @param <VID_T> inner representation type of vertex id
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 * @param <C> implementation class for {@link ContextBase}
 */
public interface AppBase<
        OID_T, VID_T, VDATA_T, EDATA_T, C extends ContextBase<OID_T, VID_T, VDATA_T, EDATA_T>> {}

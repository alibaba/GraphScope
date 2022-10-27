/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.app;

import com.alibaba.graphscope.context.GraphXParallelAdaptorContext;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graphx.GraphXParallelPIE;
import com.alibaba.graphscope.graphx.utils.SerializationUtils;
import com.alibaba.graphscope.parallel.ParallelMessageManager;

import java.net.URLClassLoader;

public class GraphXParallelAdaptor<VDATA_T, EDATA_T, MSG>
        implements ParallelAppBase<
                Long, Long, VDATA_T, EDATA_T, GraphXParallelAdaptorContext<VDATA_T, EDATA_T, MSG>> {

    public static <VD, ED, M> GraphXParallelAdaptor<VD, ED, M> createImpl(
            Class<? extends VD> vdClass, Class<? extends ED> edClass, Class<? extends M> msgClass) {
        return new GraphXParallelAdaptor<VD, ED, M>();
    }

    public static <VD, ED, M> GraphXParallelAdaptor<VD, ED, M> create(
            URLClassLoader classLoader, String serialPath) throws ClassNotFoundException {
        Object[] objects = SerializationUtils.read(classLoader, serialPath);
        if (objects.length != 10) {
            throw new IllegalStateException(
                    "Expect 10 deserialzed object, but only got " + objects.length);
        }
        Class<?> vdClass = (Class<?>) objects[0];
        Class<?> edClass = (Class<?>) objects[1];
        Class<?> msgClass = (Class<?>) objects[2];
        return (GraphXParallelAdaptor<VD, ED, M>) createImpl(vdClass, edClass, msgClass);
    }

    @Override
    public void PEval(
            IFragment<Long, Long, VDATA_T, EDATA_T> graph,
            ParallelContextBase<Long, Long, VDATA_T, EDATA_T> context,
            ParallelMessageManager messageManager) {
        GraphXParallelAdaptorContext<VDATA_T, EDATA_T, MSG> ctx =
                (GraphXParallelAdaptorContext<VDATA_T, EDATA_T, MSG>) context;
        GraphXParallelPIE<VDATA_T, EDATA_T, MSG> proxy = ctx.getGraphXProxy();
        proxy.ParallelPEval();
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            IFragment<Long, Long, VDATA_T, EDATA_T> graph,
            ParallelContextBase<Long, Long, VDATA_T, EDATA_T> context,
            ParallelMessageManager messageManager) {
        GraphXParallelAdaptorContext<VDATA_T, EDATA_T, MSG> ctx =
                (GraphXParallelAdaptorContext<VDATA_T, EDATA_T, MSG>) context;
        GraphXParallelPIE<VDATA_T, EDATA_T, MSG> proxy = ctx.getGraphXProxy();
        boolean maxIterationReached = proxy.ParallelIncEval();
        if (!maxIterationReached) {
            messageManager.ForceContinue();
        }
    }
}

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

package com.alibaba.graphscope.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.PropertyDefaultContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.PropertyMessageManager;

import org.junit.Assert;
import org.junit.Test;

public class ContextUtilsTest {

    @Test
    public void test() {
        Assert.assertTrue(
                ContextUtils.getCtxObjBaseClzName(new SampleContext())
                        .equals("LabeledVertexDataContext"));
        Assert.assertTrue(
                ContextUtils.getCtxObjBaseClzName(new SampleContext2())
                        .equals("VertexDataContext"));
    }

    public static class SampleContext extends LabeledVertexDataContext<Long, Double>
            implements PropertyDefaultContextBase<Long> {

        @Override
        public void Init(
                ArrowFragment<Long> fragment,
                PropertyMessageManager messageManager,
                JSONObject jsonObject) {}
    }

    public static class SampleContext2
            extends VertexDataContext<IFragment<Long, Long, Long, Double>, Double>
            implements DefaultContextBase<Long, Long, Long, Double> {

        /**
         * Called by grape framework, before any PEval. You can initiating data structures need
         * during super steps here.
         *
         * @param frag The graph fragment providing accesses to graph data.
         * @param messageManager The message manger which manages messages between fragments.
         * @param jsonObject String args from cmdline.
         * @see IFragment
         * @see DefaultMessageManager
         */
        @Override
        public void Init(
                IFragment<Long, Long, Long, Double> frag,
                DefaultMessageManager messageManager,
                JSONObject jsonObject) {}

        /**
         * Output will be executed when the computations finalizes. Data maintained in this context
         * shall be outputted here.
         *
         * @param frag The graph fragment contains the graph info.
         * @see IFragment
         */
        @Override
        public void Output(IFragment<Long, Long, Long, Double> frag) {}
    }
}

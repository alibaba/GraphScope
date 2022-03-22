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
import com.alibaba.graphscope.app.DefaultPropertyAppBase;
import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.PropertyDefaultContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.parallel.PropertyMessageManager;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppContextGetterTest {

    private static Logger logger = LoggerFactory.getLogger(AppContextGetterTest.class.getName());

    @Test
    public void test() {
        Class<? extends DefaultPropertyAppBase> appClass = SamplePropertyApp.class;
        SamplePropertyApp sampleApp = new SamplePropertyApp();

        logger.info(AppContextGetter.getContextName(sampleApp));
    }

    @Test
    public void test1() {
        SampleContext sampleContext = new SampleContext();
        Assert.assertTrue(
                AppContextGetter.getLabeledVertexDataContextDataType(sampleContext)
                        .equals("double"));
    }

    @Test
    public void test2() {
        Vertex<Long> prev = FFITypeFactoryhelper.newVertexLong();
        logger.info("Vertex<Long>: " + FFITypeFactoryhelper.getForeignName(prev));
    }

    public static class SampleContext extends LabeledVertexDataContext<Long, Double>
            implements PropertyDefaultContextBase<Long> {

        public SampleContext() {}

        @Override
        public void Init(
                ArrowFragment<Long> fragment,
                PropertyMessageManager messageManager,
                JSONObject jsonObject) {}
    }

    public static class SamplePropertyApp implements DefaultPropertyAppBase<Long, SampleContext> {

        @Override
        public void PEval(
                ArrowFragment<Long> fragment,
                PropertyDefaultContextBase<Long> context,
                PropertyMessageManager messageManager) {}

        @Override
        public void IncEval(
                ArrowFragment<Long> graph,
                PropertyDefaultContextBase<Long> context,
                PropertyMessageManager messageManager) {}
    }
}

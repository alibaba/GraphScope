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

import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.LabeledVertexPropertyContext;
import com.alibaba.graphscope.context.ProjectedDefaultContextBase;
import com.alibaba.graphscope.context.PropertyDefaultContextBase;
import com.alibaba.graphscope.context.PropertyParallelContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.context.VertexPropertyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextUtils {
    private static Logger logger = LoggerFactory.getLogger(ContextUtils.class.getName());

    public static String getPropertyCtxObjBaseClzName(PropertyDefaultContextBase ctxObj) {
        if (ctxObj instanceof LabeledVertexDataContext) {
            return "LabeledVertexDataContext";
        } else if (ctxObj instanceof LabeledVertexPropertyContext) {
            return "LabeledVertexPropertyContext";
        }
        logger.error(
                "Cannot find the base class name for "
                        + ctxObj
                        + " of class"
                        + ctxObj.getClass().getName());
        return null;
    }

    public static String getPropertyCtxObjBaseClzName(PropertyParallelContextBase ctxObj) {
        if (ctxObj instanceof LabeledVertexDataContext) {
            return "LabeledVertexDataContext";
        } else if (ctxObj instanceof LabeledVertexPropertyContext) {
            return "LabeledVertexPropertyContext";
        }
        logger.error(
                "Cannot find the base class name for "
                        + ctxObj
                        + " of class"
                        + ctxObj.getClass().getName());
        return null;
    }

    public static String getProjectedCtxObjBaseClzName(ProjectedDefaultContextBase ctxObj) {
        if (ctxObj instanceof VertexDataContext) {
            return "VertexDataContext";
        } else if (ctxObj instanceof VertexPropertyContext) {
            return "VertexPropertyContext";
        }
        logger.error(
                "Cannot find the base class name for "
                        + ctxObj
                        + " of class"
                        + ctxObj.getClass().getName());
        return null;
    }
}

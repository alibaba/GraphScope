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

package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.exception.GremlinResultParserException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public enum GremlinResultParserFactory implements GremlinResultParser {
    GRAPH_ELEMENT {
        @Override
        public Object parseFrom(IrResult.Results results) {
            IrResult.Element element = ParserUtils.getHeadEntry(results).getElement();
            Object graphElement = ParserUtils.parseElement(element);
            if (!(graphElement instanceof Element || graphElement instanceof List)) {
                throw new GremlinResultParserException(
                        "parse element should return vertex or edge or graph path");
            }
            return graphElement;
        }
    },

    SINGLE_VALUE {
        @Override
        public Object parseFrom(IrResult.Results results) {
            IrResult.Entry entry = ParserUtils.getHeadEntry(results);
            return ParserUtils.parseEntry(entry);
        }
    },

    SUBGRAPH {
        @Override
        public Object parseFrom(IrResult.Results results) {
            return EmptyValue.INSTANCE;
        }
    };

    private static Logger logger = LoggerFactory.getLogger(GremlinResultParserFactory.class);
}

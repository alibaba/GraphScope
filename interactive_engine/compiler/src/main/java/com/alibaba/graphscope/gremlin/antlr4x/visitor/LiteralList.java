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

package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.ObjectUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * parse a list of literals from {@code OC_ListLiteralContext} or {@code OC_ExpressionContext}(s)
 */
public class LiteralList {
    private final GremlinGSParser.OC_ListLiteralContext literalCtx;
    private final List<GremlinGSParser.OC_ExpressionContext> exprCtxList;

    public LiteralList(
            GremlinGSParser.OC_ListLiteralContext literalCtx,
            List<GremlinGSParser.OC_ExpressionContext> exprCtxList) {
        this.literalCtx = literalCtx;
        this.exprCtxList = exprCtxList;
    }

    public boolean isEmpty() {
        return literalCtx == null && ObjectUtils.isEmpty(exprCtxList);
    }

    public <T> List<T> toList(Class<T> clazz) {
        List<T> literals = Lists.newArrayList();
        if (literalCtx != null) {
            List<Object> objs = (List) LiteralVisitor.INSTANCE.visit(literalCtx);
            literals.addAll(objs.stream().map(clazz::cast).collect(Collectors.toList()));
        } else if (ObjectUtils.isNotEmpty(exprCtxList)) {
            for (GremlinGSParser.OC_ExpressionContext exprCtx : exprCtxList) {
                if (exprCtx != null) {
                    Object value = LiteralVisitor.INSTANCE.visit(exprCtx);
                    Preconditions.checkArgument(
                            clazz.isInstance(value),
                            "value type ["
                                    + value.getClass()
                                    + "] mismatch with the expected type ["
                                    + clazz
                                    + "]");
                    literals.add(clazz.cast(value));
                }
            }
        }
        return literals;
    }
}

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
package com.alibaba.graphscope.gaia;

import com.alibaba.graphscope.common.proto.Gremlin;

public class FilterChainHelper {
    public static Gremlin.FilterChain.Builder createFilterChain() {
        return Gremlin.FilterChain.newBuilder();
    }

    public static void and(final Gremlin.FilterChain.Builder chain, final Gremlin.FilterChain add) {
        int size = chain.getNodeCount();
        if (size > 0) {
            chain.getNodeBuilder(size - 1).setNext(Gremlin.Connect.AND);
        }
        if (hasConnect(add, Gremlin.Connect.OR)) {
            Gremlin.FilterNode.Builder newone = Gremlin.FilterNode.newBuilder().setChain(add.toByteString()).setNext(Gremlin.Connect.OR);
            chain.addNode(newone);
        } else {
            chain.addAllNode(add.getNodeList());
        }
    }

    public static void or(final Gremlin.FilterChain.Builder chain, final Gremlin.FilterChain add) {
        int size = chain.getNodeCount();
        if (size > 0) {
            chain.getNodeBuilder(size - 1).setNext(Gremlin.Connect.OR);
        }
        if (hasConnect(add, Gremlin.Connect.AND)) {
            Gremlin.FilterNode.Builder newone = Gremlin.FilterNode.newBuilder().setChain(add.toByteString()).setNext(Gremlin.Connect.OR);
            chain.addNode(newone);
        } else {
            chain.addAllNode(add.getNodeList());
        }
    }

    public static boolean hasConnect(final Gremlin.FilterChain chain, Gremlin.Connect connect) {
        for (int i = 0; i < chain.getNodeCount() - 1; ++i) {
            if (chain.getNode(i).getNext() == connect) return true;
        }
        return false;
    }
}



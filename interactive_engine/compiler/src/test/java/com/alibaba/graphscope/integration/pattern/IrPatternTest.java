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

package com.alibaba.graphscope.integration.pattern;

import com.alibaba.graphscope.gremlin.integration.graph.RemoteTestGraph;
import com.alibaba.graphscope.gremlin.integration.graph.RemoteTestGraphProvider;
import com.alibaba.graphscope.gremlin.integration.suite.pattern.IrPatternTestSuite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

@RunWith(IrPatternTestSuite.class)
@GraphProviderClass(provider = RemoteTestGraphProvider.class, graph = RemoteTestGraph.class)
public class IrPatternTest {}

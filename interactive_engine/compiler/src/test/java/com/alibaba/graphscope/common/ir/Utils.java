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

package com.alibaba.graphscope.common.ir;

import com.alibaba.graphscope.common.ir.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.schema.GraphSchemaWrapper;
import com.alibaba.graphscope.common.ir.schema.StatisticSchema;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.utils.FileUtils;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

public class Utils {
    public static final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    public static final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    public static final StatisticSchema schema = mockSchemaMeta();

    public static final GraphBuilder mockGraphBuilder() {
        GraphOptCluster cluster = GraphOptCluster.create(rexBuilder);
        return GraphBuilder.create(null, cluster, new GraphOptSchema(cluster, schema));
    }

    private static StatisticSchema mockSchemaMeta() {
        String json = FileUtils.readJsonFromResource("schema/modern.json");
        return new GraphSchemaWrapper(
                com.alibaba.graphscope.common.ir.schema.Utils.buildSchemaFromJson(json), false);
    }
}

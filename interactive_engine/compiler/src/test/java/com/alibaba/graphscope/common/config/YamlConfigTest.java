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

package com.alibaba.graphscope.common.config;

import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.procedure.GraphStoredProcedures;
import com.alibaba.graphscope.common.ir.meta.procedure.StoredProcedureMeta;
import com.alibaba.graphscope.common.ir.meta.reader.LocalIrMetaReader;
import com.alibaba.graphscope.common.ir.meta.schema.IrGraphSchema;

import org.junit.Assert;
import org.junit.Test;

public class YamlConfigTest {
    @Test
    public void hiactor_config_test() throws Exception {
        YamlConfigs configs =
                new YamlConfigs("config/gs_interactive_hiactor.yaml", FileLoadType.RESOURCES);
        Assert.assertEquals("localhost:8001", HiactorConfig.HIACTOR_HOSTS.get(configs));
    }

    @Test
    public void procedure_config_test() throws Exception {
        YamlConfigs configs =
                new YamlConfigs("config/gs_interactive_hiactor.yaml", FileLoadType.RESOURCES);
        IrMeta irMeta = new LocalIrMetaReader(configs).readMeta();
        GraphStoredProcedures procedures = irMeta.getStoredProcedures();
        StoredProcedureMeta meta = procedures.getStoredProcedure("ldbc_ic2");
        Assert.assertEquals(
                "StoredProcedureMeta{name='ldbc_ic2', returnType=RecordType(CHAR(1) name),"
                        + " parameters=[Parameter{name='personId2', dataType=BIGINT},"
                        + " Parameter{name='maxDate', dataType=BIGINT}], option={type=x_cypher,"
                        + " query=MATCH(n: PERSON ${personId2}) WHERE n.creationDate < ${maxDate}"
                        + " RETURN n.firstName AS name LIMIT 10;}}",
                meta.toString());
    }

    @Test
    public void pegasus_config_test() throws Exception {
        YamlConfigs configs =
                new YamlConfigs("config/gs_interactive_pegasus.yaml", FileLoadType.RESOURCES);
        Assert.assertEquals(
                "PlannerConfig{isOn=true, opt=RBO, rules=[FilterMatchRule], glogueSize=3}",
                (new PlannerConfig(configs)).toString());
        Assert.assertEquals(
                "localhost:8001, localhost:8005", PegasusConfig.PEGASUS_HOSTS.get(configs));
        Assert.assertEquals(3, (int) PegasusConfig.PEGASUS_WORKER_NUM.get(configs));
        Assert.assertEquals(2048, (int) PegasusConfig.PEGASUS_BATCH_SIZE.get(configs));
        Assert.assertEquals(18, (int) PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(configs));
        Assert.assertEquals(
                "./target/test-classes/config/modern/graph.yaml",
                GraphConfig.GRAPH_META_SCHEMA_URI.get(configs));
        Assert.assertEquals("pegasus", FrontendConfig.ENGINE_TYPE.get(configs));
        Assert.assertEquals(false, FrontendConfig.GREMLIN_SERVER_DISABLED.get(configs));
        Assert.assertEquals(8003, (int) FrontendConfig.GREMLIN_SERVER_PORT.get(configs));
        Assert.assertEquals(false, FrontendConfig.NEO4J_BOLT_SERVER_DISABLED.get(configs));
        Assert.assertEquals(8002, (int) FrontendConfig.NEO4J_BOLT_SERVER_PORT.get(configs));
        Assert.assertEquals(200, (int) FrontendConfig.QUERY_EXECUTION_TIMEOUT_MS.get(configs));
    }

    @Test
    public void schema_config_test() throws Exception {
        YamlConfigs configs =
                new YamlConfigs("config/gs_interactive_hiactor.yaml", FileLoadType.RESOURCES);
        IrMeta irMeta = new LocalIrMetaReader(configs).readMeta();
        IrGraphSchema graphSchema = irMeta.getSchema();
        Assert.assertEquals(
                "DefaultGraphVertex{labelId=0, label=person,"
                        + " propertyList=[DefaultGraphProperty{id=0, name=id, dataType=LONG},"
                        + " DefaultGraphProperty{id=1, name=name, dataType=STRING},"
                        + " DefaultGraphProperty{id=2, name=age, dataType=INT}],"
                        + " primaryKeyList=[id]}",
                graphSchema.getElement("person").toString());
        Assert.assertEquals(
                "DefaultGraphVertex{labelId=1, label=software,"
                        + " propertyList=[DefaultGraphProperty{id=0, name=id, dataType=LONG},"
                        + " DefaultGraphProperty{id=1, name=name, dataType=STRING},"
                        + " DefaultGraphProperty{id=2, name=lang, dataType=STRING},"
                        + " DefaultGraphProperty{id=3, name=creationDate, dataType=DATE}],"
                        + " primaryKeyList=[id]}",
                graphSchema.getElement("software").toString());
    }

    @Test
    public void compiler_config_test() throws Exception {
        YamlConfigs configs =
                new YamlConfigs("config/gs_interactive_hiactor.yaml", FileLoadType.RESOURCES);
        Assert.assertEquals("UTF-8", FrontendConfig.CALCITE_DEFAULT_CHARSET.get(configs));
    }
}

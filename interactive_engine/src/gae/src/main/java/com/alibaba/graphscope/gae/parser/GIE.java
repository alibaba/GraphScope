package com.alibaba.graphscope.gae.parser;

import com.alibaba.graphscope.gaia.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.Iterator;
import java.util.Map;

public enum GIE implements Generator {
    GREMLIN_QUERY {
        @Override
        public Map<String, Object> generate(Map<String, Object> args) {
            String json = readFileFromResource("gie.gremlin.query.json");
            Map<String, Object> gremlin = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {
            });
            Traversal traversal = getTraversal(args);
            // remove process/scatter/gather/with from bytecode
            Bytecode gremlinCode = traversal.asAdmin().getBytecode();
            Iterator<Bytecode.Instruction> iterator = gremlinCode.getInstructions().iterator();
            while (iterator.hasNext()) {
                if (isExtendBytecode(iterator.next())) {
                    iterator.remove();
                }
            }
            String gremlinQuery = GroovyTranslator.of("g").translate(traversal.asAdmin().getBytecode());
            Map params = (Map) gremlin.get("params");
            params.put("query", gremlinQuery);
            return gremlin;
        }

        private boolean isExtendBytecode(Bytecode.Instruction instruction) {
            if (instruction.getOperator().equals("with")
                    || instruction.getOperator().equals("sample")
                    || instruction.getOperator().equals("toTensorFlowDataset")
                    || instruction.getOperator().equals("withProperty")
                    || instruction.getOperator().equals("process")
                    || instruction.getOperator().equals("scatter")
                    || instruction.getOperator().equals("gather")
                    || instruction.getOperator().equals("expr")) return true;
            return false;
        }
    }
}

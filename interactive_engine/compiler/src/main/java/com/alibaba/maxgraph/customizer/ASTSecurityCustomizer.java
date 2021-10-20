/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.customizer;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCustomizer;
import org.codehaus.groovy.ast.stmt.BlockStatement;
import org.codehaus.groovy.ast.stmt.ExpressionStatement;
import org.codehaus.groovy.ast.stmt.ReturnStatement;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.codehaus.groovy.syntax.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright ©Alibaba Inc.
 *
 * @author: ruoang
 * @date: 2019-08-12
 * @time: 18:00
 */
public class ASTSecurityCustomizer implements GroovyCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(ASTSecurityCustomizer.class);
    private static final Pattern PATTERN_DSL = Pattern.compile("\\s*g\\..*");
    private static final Pattern PATTERN_DLL = Pattern.compile("\\s*graph\\..*");
    private static final Pattern CLOSURE_FILTER_FORM = Pattern.compile("\\s*\\(it.*\\)");
    private static final Pattern CLOSURE_MAP_FORM = Pattern.compile("\\s*it.*");
    private static final Pattern GREMLIN_SCRIPT_ENGINE_FORM = Pattern.compile("\\s*gremlinscriptengine__.*");
    private static final String ESCAPED_DSL = "(1 + 1)";

    @Override
    public CompilationCustomizer create() {
        SecureASTCustomizer customizer = new SecureASTCustomizer();
        customizer.setClosuresAllowed(true);
        customizer.setMethodDefinitionAllowed(false);
        customizer.setPackageAllowed(false);
        customizer.setIndirectImportCheckEnabled(true);

        List<Integer> tokenLists = new ArrayList<>();
        tokenLists.add(Types.EQUAL);
        customizer.setTokensBlacklist(tokenLists);

        customizer.addStatementCheckers(statement -> {
            if (statement instanceof BlockStatement) {
                return true;
            }
            if (statement instanceof ReturnStatement) {
                return true;
            }
            if (statement instanceof ExpressionStatement) {
                if (ESCAPED_DSL.equals(statement.getText())) {
                    return true;
                }
                boolean isValid = PATTERN_DSL.matcher(statement.getText()).matches() ||
                        PATTERN_DLL.matcher(statement.getText()).matches() ||
                        CLOSURE_FILTER_FORM.matcher(statement.getText()).matches() ||
                        CLOSURE_MAP_FORM.matcher(statement.getText()).matches() ||
                        GREMLIN_SCRIPT_ENGINE_FORM.matcher(statement.getText()).matches();
                if (!isValid) {
                    LOG.error("Invalid query: " + statement.getText());
                }
                return isValid;
            }
            LOG.error("Invalid query: " + statement.getText() + " clazz:" + statement.getClass());
            return false;
        });
        return customizer;
    }
}

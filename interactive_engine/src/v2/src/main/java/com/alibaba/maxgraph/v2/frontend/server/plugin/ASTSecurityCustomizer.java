package com.alibaba.maxgraph.v2.frontend.server.plugin;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCustomizer;
import org.codehaus.groovy.ast.stmt.BlockStatement;
import org.codehaus.groovy.ast.stmt.ExpressionStatement;
import org.codehaus.groovy.ast.stmt.ReturnStatement;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.codehaus.groovy.syntax.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Custom security for maxgraph
 */
public class ASTSecurityCustomizer implements GroovyCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(ASTSecurityCustomizer.class);
    private static final Pattern PATTERN_DSL = Pattern.compile("\\s*g\\..*");
    private static final Pattern PATTERN_DLL = Pattern.compile("\\s*graph\\..*");
    private static final String ESCAPED_DSL = "(1 + 1)";

    @Override
    public CompilationCustomizer create() {
        SecureASTCustomizer customizer = new SecureASTCustomizer();
        customizer.setClosuresAllowed(false);
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
                        PATTERN_DLL.matcher(statement.getText()).matches();
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

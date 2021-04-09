package com.alibaba.maxgraph.v2.frontend.server.plugin;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyCustomizer;
import org.codehaus.groovy.ast.AnnotatedNode;
import org.codehaus.groovy.ast.ClassCodeVisitorSupport;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.classgen.GeneratorContext;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilePhase;
import org.codehaus.groovy.control.SourceUnit;
import org.codehaus.groovy.control.customizers.CompilationCustomizer;
import org.codehaus.groovy.syntax.SyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom annotation for maxgraph
 */
public class AnnotationCustomizer implements GroovyCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(AnnotationCustomizer.class);

    @Override
    public CompilationCustomizer create() {
        return new CompilationCustomizer(CompilePhase.CONVERSION) {
            @Override
            public void call(SourceUnit source, GeneratorContext context, ClassNode classNode)
                    throws CompilationFailedException {
                classNode.visitContents(new ClassCodeVisitorSupport() {
                    @Override
                    protected SourceUnit getSourceUnit() {
                        return source;
                    }

                    @Override
                    public void visitAnnotations(AnnotatedNode node) {
                        if (!node.getAnnotations().isEmpty()) {
                            LOG.error("annotation: " + (source.getSource()));
                            source.addError(new SyntaxException("annotation is disable", classNode.getLineNumber(),
                                    classNode.getColumnNumber()));
                        }
                    }
                });
            }
        };
    }
}

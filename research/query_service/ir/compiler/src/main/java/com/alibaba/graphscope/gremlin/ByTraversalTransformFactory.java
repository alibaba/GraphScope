package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.exception.OpArgIllegalException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;

public class ByTraversalTransformFactory {
    public static String getTagByTraversalAsExpr(String tag, Traversal.Admin byTraversal) {
        String expr;
        if (byTraversal == null || byTraversal instanceof IdentityTraversal) {
            expr = "@" + tag;
        } else if (byTraversal instanceof ValueTraversal) {
            String property = ((ValueTraversal) byTraversal).getPropertyKey();
            expr = String.format("@%s.%s", tag, property);
        } else if (byTraversal.getSteps().size() == 1 && byTraversal.getStartStep() instanceof PropertiesStep) {
            String[] mapKeys = ((PropertiesStep) byTraversal.getStartStep()).getPropertyKeys();
            if (mapKeys.length == 0) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "values() is unsupported");
            }
            if (mapKeys.length > 1) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                        "use valueMap(..) instead if there are multiple project keys");
            }
            expr = String.format("@%s.%s", tag, mapKeys[0]);
        } else if (byTraversal.getSteps().size() == 1 && byTraversal.getStartStep() instanceof PropertyMapStep) {
            String[] mapKeys = ((PropertyMapStep) byTraversal.getStartStep()).getPropertyKeys();
            if (mapKeys.length > 0) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("{");
                for (int i = 0; i < mapKeys.length; ++i) {
                    if (i > 0) {
                        stringBuilder.append(", ");
                    }
                    stringBuilder.append(String.format("@%s.%s", tag, mapKeys[i]));
                }
                stringBuilder.append("}");
                expr = stringBuilder.toString();
            } else {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "valueMap() is unsupported");
            }
        } else {
            throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                    "supported pattern is [by()] or [by('name')] or [by(values('name'))]");
        }
        return expr;
    }
}

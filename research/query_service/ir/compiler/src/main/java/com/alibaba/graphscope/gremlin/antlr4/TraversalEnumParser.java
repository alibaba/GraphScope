package com.alibaba.graphscope.gremlin.antlr4;

import org.antlr.v4.runtime.tree.ParseTree;

public class TraversalEnumParser {

    /**
     * Parse enum text from a parse tree context into a enum object
     *
     * @param enumType : class of enum
     * @param context  : parse tree context
     * @return enum object
     */
    public static <E extends Enum<E>, C extends ParseTree> E parseTraversalEnumFromContext(final Class<E> enumType, final C context) {
        final String text = context.getText();
        final String className = enumType.getSimpleName();

        // Support qualified class names like (ex: T.id or Scope.local)
        if (text.startsWith(className)) {
            final String strippedText = text.substring(className.length() + 1);
            return E.valueOf(enumType, strippedText);
        } else {
            return E.valueOf(enumType, text);
        }
    }
}

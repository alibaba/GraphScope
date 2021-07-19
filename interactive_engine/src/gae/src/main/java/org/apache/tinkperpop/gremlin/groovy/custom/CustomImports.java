package org.apache.tinkperpop.gremlin.groovy.custom;

import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.javatuples.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class CustomImports {

    private final static Set<Class> CLASS_IMPORTS = new LinkedHashSet<>();
    private final static Set<Field> FIELD_IMPORTS = new LinkedHashSet<>();
    private final static Set<Method> METHOD_IMPORTS = new LinkedHashSet<>();
    private final static Set<Enum> ENUM_IMPORTS = new LinkedHashSet<>();

    static {
        /////////////
        // CLASSES //
        /////////////


        // graph traversal

        CLASS_IMPORTS.add(___.class);

        /////////////
        // METHODS //
        /////////////


        // uniqueMethods(AnonymousTraversalSource.class).forEach(METHOD_IMPORTS::add);
        uniqueMethods(___.class).filter(m -> !m.getName().equals("___")).forEach(METHOD_IMPORTS::add);

    }

    private CustomImports() {
        // static methods only, do not instantiate class
    }

    public static Set<Class> getClassImports() {
        return Collections.unmodifiableSet(CLASS_IMPORTS);
    }

    public static Set<Method> getMethodImports() {
        return Collections.unmodifiableSet(METHOD_IMPORTS);
    }

    public static Set<Enum> getEnumImports() {
        return Collections.unmodifiableSet(ENUM_IMPORTS);
    }

    public static Set<Field> getFieldImports() {
        return Collections.unmodifiableSet(FIELD_IMPORTS);
    }

    /**
     * Filters to unique method names on each class.
     */
    private static Stream<Method> uniqueMethods(final Class<?> clazz) {
        final Set<String> unique = new LinkedHashSet<>();
        return Stream.of(clazz.getMethods())
                .filter(m -> Modifier.isStatic(m.getModifiers()))
                .map(m -> Pair.with(generateMethodDescriptor(m), m))
                .filter(p -> {
                    final boolean exists = unique.contains(p.getValue0());
                    if (!exists) unique.add(p.getValue0());
                    return !exists;
                })
                .map(Pair::getValue1);
    }

    private static String generateMethodDescriptor(final Method m) {
        return m.getDeclaringClass().getCanonicalName() + "." + m.getName();
    }
}

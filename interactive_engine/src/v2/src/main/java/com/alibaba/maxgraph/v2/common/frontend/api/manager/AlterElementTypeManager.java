package com.alibaba.maxgraph.v2.common.frontend.api.manager;

/**
 * Alter element type interface, contains add/drop properties
 *
 * @param <M> The subclass of {@link AlterElementTypeManager}
 */
public interface AlterElementTypeManager<M extends AlterElementTypeManager> extends AddElementPropertyManager<M>, DropElementPropertyManager<M>, SchemaManager {
}

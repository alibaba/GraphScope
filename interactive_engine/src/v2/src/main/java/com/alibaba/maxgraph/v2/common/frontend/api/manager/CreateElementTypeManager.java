package com.alibaba.maxgraph.v2.common.frontend.api.manager;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;

import java.util.List;

/**
 * Interface of element type, contains add property methods
 *
 * @param <M> The subclass of {@link CreateElementTypeManager}
 */
public interface CreateElementTypeManager<M extends CreateElementTypeManager> extends SchemaManager, AddElementPropertyManager<M> {
}

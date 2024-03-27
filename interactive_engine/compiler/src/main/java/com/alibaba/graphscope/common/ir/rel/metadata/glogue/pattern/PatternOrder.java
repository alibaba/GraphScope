package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

/// PatternOrder is used to reorder the Pattern, and assign a new order id to each PatternVertex.
/// A good pattern order can help to reduce the search space of the pattern matching.
public abstract class PatternOrder {
    /// Given a PatternVertex v, return its new order after pattern ordering.
    public abstract Integer getVertexOrder(PatternVertex vertex);
    /// Given a PatternVertex v, return its group id after pattern ordering.
    /// Notice if two vertices have the same group id, they must structurally equivalent.
    public abstract Integer getVertexGroup(PatternVertex vertex);
    /// Given a order id, return the PatternVertex with the order id after pattern ordering.
    public abstract PatternVertex getVertexByOrder(Integer id);
}

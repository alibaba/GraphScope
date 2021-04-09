package com.alibaba.maxgraph.v2.frontend.compiler.tree.addition;

/**
 * Determine whether sum/count should output zero value
 */
public interface JoinZeroNode {
    void disableJoinZero();

    void enableJoinZero();

    boolean isJoinZeroFlag();
}

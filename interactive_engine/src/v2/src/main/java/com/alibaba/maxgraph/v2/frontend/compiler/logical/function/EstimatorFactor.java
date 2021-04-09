package com.alibaba.maxgraph.v2.frontend.compiler.logical.function;

public class EstimatorFactor {
    private double outputFactor = 1;    // factor for output number from input

    public double getOutputFactor() {
        return outputFactor;
    }

    public void setOutputFactor(double outputFactor) {
        this.outputFactor = outputFactor;
    }
}

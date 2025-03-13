package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

public abstract class DegreeDistribution {

    public abstract void initialize(Configuration conf);

    public abstract void reset(long seed);

    public abstract long nextDegree();

    public double mean(long numPersons) {
        return -1;
    }
}

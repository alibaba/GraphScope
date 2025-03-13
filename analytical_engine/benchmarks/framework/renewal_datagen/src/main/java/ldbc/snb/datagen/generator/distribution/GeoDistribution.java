package ldbc.snb.datagen.generator.distribution;

import org.apache.commons.math3.distribution.GeometricDistribution;
import org.apache.hadoop.conf.Configuration;


public class GeoDistribution extends DegreeDistribution {

    private GeometricDistribution geo_;
    private double ALPHA_ = 0.12;

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.GeoDistribution.alpha", ALPHA_);
        geo_ = new GeometricDistribution(ALPHA_);
    }

    public void reset(long seed) {
        geo_.reseedRandomGenerator(seed);
    }

    public long nextDegree() {
        return geo_.sample();
    }

    @Override
    public double mean(long numPersons) {
        return geo_.getNumericalMean();
    }
}

package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

import java.util.Random;

public class MoeZipfDistribution extends DegreeDistribution {

    private org.apache.commons.math3.distribution.ZipfDistribution zipf_;
    private double ALPHA_ = 1.7;
    private double DELTA_ = 1.5;
    private Random random_;

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.MoeZipfDistribution.alpha", ALPHA_);
        DELTA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.MoeZipfDistribution.delta", DELTA_);
        zipf_ = new org.apache.commons.math3.distribution.ZipfDistribution(5000, ALPHA_);
        random_ = new Random();
    }

    public void reset(long seed) {
        random_.setSeed(seed);
        zipf_.reseedRandomGenerator(seed);
    }

    public long nextDegree() {
        double prob = random_.nextDouble();
        double prime = (prob * DELTA_) / (1 + prob * (DELTA_ - 1));
        long ret = zipf_.inverseCumulativeProbability(prime);
        return ret;
    }

}


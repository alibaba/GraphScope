package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.tools.Bucket;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class BucketedDistribution extends DegreeDistribution {

    private List<Bucket> buckets_;
    private List<Random> randomDegree_;
    private Random randomPercentile_;

    public abstract List<Bucket> getBuckets(Configuration conf);

    public void initialize(Configuration conf) {
        buckets_ = this.getBuckets(conf);
        randomPercentile_ = new Random(0);
        randomDegree_ = new ArrayList<>();
        for (int i = 0; i < buckets_.size(); i++) {
            randomDegree_.add(new Random(0));
        }
    }

    public void reset(long seed) {
        Random seedRandom = new Random(53223436L + 1234567 * seed);
        for (int i = 0; i < buckets_.size(); i++) {
            randomDegree_.get(i).setSeed(seedRandom.nextLong());
        }
        randomPercentile_.setSeed(seedRandom.nextLong());
    }

    public long nextDegree() {
        int idx = randomPercentile_.nextInt(buckets_.size());
        double minRange = (buckets_.get(idx).min());
        double maxRange = (buckets_.get(idx).max());
        if (maxRange < minRange) {
            maxRange = minRange;
        }
        long ret = randomDegree_.get(idx).nextInt((int) maxRange - (int) minRange + 1) + (int) minRange;
        return ret;
    }

    @Override
    public double mean(long numPersons) {
        return -1.0;
    }
}

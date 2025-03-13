package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Random;

public abstract class CumulativeBasedDegreeDistribution extends DegreeDistribution {

    private List<CumulativeEntry> cumulativeProbability_;
    private Random random_;

    public class CumulativeEntry {
        public double prob_;
        public int value_;
    }

    public void initialize(Configuration conf) {
        cumulativeProbability_ = cumulativeProbability(conf);
        random_ = new Random();
    }

    public void reset(long seed) {
        random_.setSeed(seed);
    }

    public long nextDegree() {
        double prob = random_.nextDouble();
        int index = binarySearch(cumulativeProbability_, prob);
        return cumulativeProbability_.get(index).value_;
    }

    private int binarySearch(List<CumulativeEntry> cumulative, double prob) {
        int upperBound = cumulative.size() - 1;
        int lowerBound = 0;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (cumulative.get(midPoint).prob_ > prob) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    public abstract List<CumulativeEntry> cumulativeProbability(Configuration conf);
}

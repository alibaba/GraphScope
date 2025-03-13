package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ZipfDistribution extends DegreeDistribution {

    private org.apache.commons.math3.distribution.ZipfDistribution zipf_;
    private double ALPHA_ = 2.0;
    private Random random = new Random();
    private Map<Integer, Integer> histogram = new HashMap<>();
    private double probabilities[];
    private Integer values[];
    private double mean_ = 0.0;
    private int maxDegree = 1000;
    private int numSamples = 10000;

    public void initialize(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.ZipfDistribution.alpha", ALPHA_);
        zipf_ = new org.apache.commons.math3.distribution.ZipfDistribution(maxDegree, ALPHA_);
        for (int i = 0; i < numSamples; ++i) {
            int next = zipf_.sample();
            Integer currentValue = histogram.put(next, 1);
            if (currentValue != null) {
                histogram.put(next, currentValue + 1);
            }
        }
        int numDifferentValues = histogram.keySet().size();
        probabilities = new double[numDifferentValues];
        values = new Integer[numDifferentValues];
        histogram.keySet().toArray(values);
        Arrays.sort(values, Comparator.comparingInt(o -> o));

        probabilities[0] = histogram.get(values[0]) / (double) numSamples;
        for (int i = 1; i < numDifferentValues; ++i) {
            int occurrences = histogram.get(values[i]);
            double prob = occurrences / (double) numSamples;
            mean_ += prob * values[i];
            probabilities[i] = probabilities[i - 1] + prob;
        }
    }

    public void reset(long seed) {
        zipf_.reseedRandomGenerator(seed);
        random.setSeed(seed);
    }

    public long nextDegree() {
        int min = 0;
        int max = probabilities.length;
        double prob = random.nextDouble();
        int currentPosition = (max - min) / 2 + min;
        while (max > (min + 1)) {
            if (probabilities[currentPosition] > prob) {
                max = currentPosition;
            } else {
                min = currentPosition;
            }
            currentPosition = (max - min) / 2 + min;
        }
        return values[currentPosition];
    }

    public double mean(long numPersons) {
        return mean_;
    }
}

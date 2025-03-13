package ldbc.snb.datagen.generator.tools;

import ldbc.snb.datagen.DatagenParams;
import org.apache.commons.math3.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class Bucket {

    private double min_;
    private double max_;

    public static List<Bucket> bucketizeHistogram(List<Pair<Integer, Integer>> histogram, int num_buckets) {

        List<Bucket> buckets = new ArrayList<>();
        int population = 0;
        int num_edges = 0;
        for (Pair<Integer, Integer> i : histogram) {
            population += i.getValue();
            num_edges += i.getValue() * i.getKey();
        }
        num_edges /= 2;


        int avgDegreeAt1B = 200;
        int avgDegree = num_edges / population;
        double aCoeff = Math.log(avgDegreeAt1B) / Math.log(1000000000);
        double bCoeff = (aCoeff - (Math.log(avgDegree) / Math.log(population))) / Math.log10(population);

        int target_mean = (int) Math.round(Math.pow(DatagenParams.numPersons, (aCoeff - bCoeff * Math
                .log10(DatagenParams.numPersons))));
        System.out.println("Distribution mean degree: " + avgDegree + " Distribution target mean " + target_mean);
        int bucket_size = (int) (Math.ceil(population / (double) (num_buckets)));
        int current_histogram_index = 0;
        int current_histogram_left = histogram.get(current_histogram_index).getValue();
        for (int i = 0; i < num_buckets && (current_histogram_index < histogram.size()); ++i) {
            int current_bucket_count = 0;
            int min = population;
            int max = 0;
            while (current_bucket_count < bucket_size && current_histogram_index < histogram.size()) {
                int degree = histogram.get(current_histogram_index).getKey();
                min = degree < min ? degree : min;
                max = degree > max ? degree : max;
                if ((bucket_size - current_bucket_count) > current_histogram_left) {
                    current_bucket_count += current_histogram_left;
                    current_histogram_index++;
                    if (current_histogram_index < histogram.size()) {
                        current_histogram_left = histogram.get(current_histogram_index).getValue();
                    }
                } else {
                    current_histogram_left -= (bucket_size - current_bucket_count);
                    current_bucket_count = bucket_size;
                }
            }
            min = (int) (min * target_mean / (double) avgDegree);
            max = (int) (max * target_mean / (double) avgDegree);
            buckets.add(new Bucket(min, max));
        }
        return buckets;
    }


    public Bucket(double min, double max) {
        this.min_ = min;
        this.max_ = max;
    }

    public double min() {
        return min_;
    }

    public void min(double min) {
        min_ = min;
    }

    public double max() {
        return max_;
    }

    public void max(double max) {
        max_ = max;
    }
}

package ldbc.snb.datagen.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Distribution {

    private double[] distribution;
    private String distributionFile;

    public Distribution(String distributionFile) {
        this.distributionFile = distributionFile;
    }

    public void initialize() {
        try {
            BufferedReader distributionBuffer = new BufferedReader(new InputStreamReader(getClass()
                                                                                                 .getResourceAsStream(distributionFile), "UTF-8"));
            List<Double> temp = new ArrayList<>();
            String line;
            while ((line = distributionBuffer.readLine()) != null) {
                Double prob = Double.valueOf(line);
                temp.add(prob);
            }
            distribution = new double[temp.size()];
            int index = 0;
            Iterator<Double> it = temp.iterator();
            while (it.hasNext()) {
                distribution[index] = it.next();
                ++index;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int binarySearch(double prob) {
        int upperBound = distribution.length - 1;
        int lowerBound = 0;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (distribution[midPoint] > prob) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    public double nextDouble(Random random) {
        return (double) binarySearch(random.nextDouble()) / (double) distribution.length;
    }
}

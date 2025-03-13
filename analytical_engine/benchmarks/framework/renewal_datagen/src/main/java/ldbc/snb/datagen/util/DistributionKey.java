package ldbc.snb.datagen.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DistributionKey {

    private List<Pair> distribution;
    private String distributionFile;

    static class Pair {
        private double l;
        private String r;

        public Pair(double l, String r) {
            this.l = l;
            this.r = r;
        }

        public double getL() {
            return l;
        }

        public String getR() {
            return r;
        }

        public void setL(double l) {
            this.l = l;
        }

        public void setR(String r) {
            this.r = r;
        }

    }

    public DistributionKey(String distributionFile) {
        this.distributionFile = distributionFile;
    }

    public void initialize() {
        try {
            BufferedReader distributionBuffer = new BufferedReader(new InputStreamReader(getClass()
                                                                                                 .getResourceAsStream(distributionFile), "UTF-8"));
            String line;
            distribution = new ArrayList<>();

            while ((line = distributionBuffer.readLine()) != null) {
                String[] parts = line.split(" ");
                String key = parts[0];
                Double valor = Double.valueOf(parts[1]);
                Pair p = new Pair(valor, key);
                distribution.add(p);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int binarySearch(double prob) {
        int upperBound = distribution.size() - 1;
        int lowerBound = 0;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (distribution.get(midPoint).getL() > prob) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    public String nextDouble(Random random) {
        return distribution.get(binarySearch(random.nextDouble())).getR();
    }
}

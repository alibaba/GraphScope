package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.generator.tools.Bucket;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class EmpiricalDistribution extends BucketedDistribution {

    private String fileName = null;

    @Override
    public List<Bucket> getBuckets(Configuration conf) {
        fileName = conf.get("ldbc.snb.datagen.generator.distribution.EmpiricalDistribution.fileName");
        List<Pair<Integer, Integer>> histogram = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                String data[] = line.split(" ");
                histogram.add(new Pair<>(Integer.parseInt(data[0]), Integer.parseInt(data[1])));
            }
            reader.close();
            return Bucket.bucketizeHistogram(histogram, 1000);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

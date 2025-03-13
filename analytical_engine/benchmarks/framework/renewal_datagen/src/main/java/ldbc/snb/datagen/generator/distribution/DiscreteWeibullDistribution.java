package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.DatagenParams;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

public class DiscreteWeibullDistribution extends CumulativeBasedDegreeDistribution {

    private double BETA_ = 0.8505;
    private double P_ = 0.0205;

    public List<CumulativeEntry> cumulativeProbability(Configuration conf) {
        BETA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.DiscreteWeibullDistribution.beta", BETA_);
        P_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.DiscreteWeibullDistribution.p", P_);
        List<CumulativeEntry> cumulative = new ArrayList<>();
        for (int i = 0; i < DatagenParams.numPersons; ++i) {
            //double prob = Math.pow(1.0-P_,Math.pow(i,BETA_))-Math.pow((1.0-P_),Math.pow(i+1,BETA_));
            double prob = 1.0 - Math.pow((1.0 - P_), Math.pow(i + 1, BETA_));
            CumulativeEntry entry = new CumulativeEntry();
            entry.prob_ = prob;
            entry.value_ = i + 1;
            cumulative.add(entry);
        }
        return cumulative;
    }
}

package ldbc.snb.datagen.generator.distribution;

import ldbc.snb.datagen.DatagenParams;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

public class AltmannDistribution extends CumulativeBasedDegreeDistribution {

    private double normalization_factor_ = 0.0;
    private double ALPHA_ = 0.4577;
    private double BETA_ = 0.0162;


    public List<CumulativeEntry> cumulativeProbability(Configuration conf) {
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.AltmannDistribution.alpha", ALPHA_);
        BETA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.AltmannDistribution.beta", BETA_);

        long POPULATION_ = DatagenParams.numPersons;
        for (int i = 1; i <= POPULATION_; ++i) {
            normalization_factor_ += Math.pow(i, -ALPHA_) * Math.exp(-BETA_ * i);
        }
        List<CumulativeEntry> cumulative = new ArrayList<>();
        for (int i = 1; i <= POPULATION_; ++i) {
            double prob = Math.pow(i, -ALPHA_) * Math.exp(-BETA_ * i) / normalization_factor_;
            prob += cumulative.size() > 0 ? cumulative.get(i - 2).prob_ : 0.0;
            CumulativeEntry entry = new CumulativeEntry();
            entry.prob_ = prob;
            entry.value_ = i;
            cumulative.add(entry);
        }
        return cumulative;
    }
}

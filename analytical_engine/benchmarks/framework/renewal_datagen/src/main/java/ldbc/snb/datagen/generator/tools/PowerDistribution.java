package ldbc.snb.datagen.generator.tools;

import umontreal.iro.lecuyer.probdist.PowerDist;

import java.util.Random;

public class PowerDistribution {
    private PowerDist powerDist;

    public PowerDistribution(double a, double b, double alpha) {
        powerDist = new PowerDist(a, b, alpha);
    }

    public int getValue(Random random) {
        return (int) powerDist.inverseF(random.nextDouble());
    }

    public double getDouble(Random random) {
        return powerDist.inverseF(random.nextDouble());
    }
}

package ldbc.snb.datagen.test.csv;

public class LongPairCheck extends NumericPairCheck<Long> {

    private long offsetA;
    private long offsetB;

    public LongPairCheck(Parser<Long> parser, String name, Integer columnA, Integer columnB, NumericCheckType type, long offsetA, long offsetB) {
        super(parser, name, columnA, columnB, type);
        this.offsetA = offsetA;
        this.offsetB = offsetB;
    }

    @Override
    public boolean greater(Long val1, Long val2) {
        return (val1+offsetA) > (val2+offsetB);
    }

    @Override
    public boolean greaterEqual(Long val1, Long val2) {
        return (val1+offsetA) >= (val2+offsetB);
    }

    @Override
    public boolean less(Long val1, Long val2) {
        return (val1+offsetA) < (val2+offsetB);
    }

    @Override
    public boolean lessEqual(Long val1, Long val2) {
        return (val1+offsetA) <= (val2+offsetB);
    }

    @Override
    public boolean equals(Long val1, Long val2) {
        return (val1+offsetA) == (val2+offsetB);
    }

    @Override
    public boolean nonEquals(Long val1, Long val2) {
        return (val1+offsetA) != (val2+offsetB);
    }
}

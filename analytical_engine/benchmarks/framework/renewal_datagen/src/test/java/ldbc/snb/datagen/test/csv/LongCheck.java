package ldbc.snb.datagen.test.csv;

public class LongCheck extends NumericCheck<Long> {

    private long offset = 0;

    public LongCheck(Parser<Long> parser, String name, Integer column, NumericCheckType type, Long val1, Long val2, Long offset ) {
        super(parser, name, column, type, val1, val2);
        this.offset = offset;
    }

    public LongCheck(Parser<Long> parser, String name, Integer column, NumericCheckType type, Long val1, Long val2) {
        super(parser, name, column, type, val1, val2);
    }

    @Override
    public boolean greater(Long val1, Long val2) {
        return val1+offset > val2;
    }

    @Override
    public boolean greaterEqual(Long val1, Long val2) {
        return val1+offset >= val2;
    }

    @Override
    public boolean less(Long val1, Long val2) {
        return val1+offset < val2;
    }

    @Override
    public boolean lessEqual(Long val1, Long val2) {
        return val1+offset <= val2;
    }

    @Override
    public boolean equals(Long val1, Long val2) {
        return val1+offset == val2;
    }

    @Override
    public boolean nonEquals(Long val1, Long val2) {
        return val1+offset != val2;
    }

    @Override
    public boolean between(Long val1, Long val2, Long val3) {
        return (val1+offset >= val2 && val1+offset < val3);
    }
}

package ldbc.snb.datagen.test.csv;

public class LongParser extends Parser<Long> {

    @Override
    public Long parse(String s) {
        return Long.parseLong(s);
    }
}

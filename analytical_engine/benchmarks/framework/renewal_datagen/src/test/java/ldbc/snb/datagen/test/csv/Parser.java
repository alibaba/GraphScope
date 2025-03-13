package ldbc.snb.datagen.test.csv;

public abstract class Parser<T> {
    public abstract T parse(String s);
}

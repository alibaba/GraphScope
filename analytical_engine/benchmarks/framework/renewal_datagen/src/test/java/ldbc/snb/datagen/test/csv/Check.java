package ldbc.snb.datagen.test.csv;

import java.util.List;

public abstract class Check {

    protected String checkName = null;
    protected List<Integer> columns = null;

    public String getCheckName() {
        return checkName;
    }

    public Check(String name, List<Integer> columns) {
        this.checkName = name;
        this.columns = columns;
    }

    public List<Integer> getColumns() {
        return columns;
    }

    public abstract boolean check(List<String> values);
}


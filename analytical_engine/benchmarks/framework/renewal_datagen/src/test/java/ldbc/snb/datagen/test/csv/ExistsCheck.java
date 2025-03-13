package ldbc.snb.datagen.test.csv;

import java.util.List;

public class ExistsCheck<T> extends Check {

    protected List<ColumnSet<T>> refColumns = null;
    protected Parser<T> parser = null;

    public ExistsCheck(Parser<T> parser, List<Integer> indexes, List<ColumnSet<T>> refColumns) {
        super("Exists Check", indexes);
        this.refColumns = refColumns;
        this.parser = parser;
    }

    @Override
    public boolean check(List<String> values) {
        for(String val : values) {
            boolean found = false;
            for( ColumnSet<T> column : refColumns) {
                if(column.contains(parser.parse(val))) {
                    found = true;
                    break;
                }
            }
            if(!found) return false;
        }
        return true;
    }
}

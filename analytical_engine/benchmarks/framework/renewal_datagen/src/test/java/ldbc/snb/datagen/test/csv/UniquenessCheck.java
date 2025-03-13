package ldbc.snb.datagen.test.csv;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class UniquenessCheck extends Check {

    private HashSet<String> values = null;

    public UniquenessCheck(int column) {
        super( "Uniqueness check", (new ArrayList<>()));
        this.getColumns().add(column);
        values = new HashSet<>();
    }

    @Override
    public boolean check(List<String> vals) {
        for(String value : vals) {
            if(values.contains(value)) return false;
            values.add(value);
        }
        return true;
    }
}

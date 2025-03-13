package ldbc.snb.datagen.test.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PairUniquenessCheck<T,S> extends Check {


    protected Map< T,Set<S>> values = null;
    protected Parser<T> parserA = null;
    protected Parser<S> parserB = null;


    public PairUniquenessCheck(Parser<T> parserA, Parser<S> parserB, int columnA, int columnB) {
        super( "Pair Uniqueness Check", (new ArrayList<>()));
        this.parserA = parserA;
        this.parserB = parserB;
        this.getColumns().add(columnA);
        this.getColumns().add(columnB);
        values = new HashMap<>();
    }

    @Override
    public boolean check(List<String> vals) {
        T valA = parserA.parse(vals.get(0));
        S valB = parserB.parse(vals.get(1));
        Set<S> others = values.get(valA);
        if(others == null) {
            others = new HashSet<>();
            others.add(valB);
            values.put(valA,others);
        } else {
            if(others.contains(valB)) {
                System.err.println(valA+" "+valB+" already exists");
                return false;
            }
            others.add(valB);
        }
        return true;
    }
}

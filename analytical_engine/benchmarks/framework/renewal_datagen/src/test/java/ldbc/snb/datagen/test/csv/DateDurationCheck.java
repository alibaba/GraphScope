package ldbc.snb.datagen.test.csv;

import ldbc.snb.datagen.util.DateUtils;

import java.util.ArrayList;
import java.util.List;

public class DateDurationCheck extends Check {

    private Long val1;
    private Long val2;

    public DateDurationCheck(String name, Integer baseColumn, Integer offsetColumn, Long val1, Long val2 ) {
        super(name, new ArrayList<>());
        getColumns().add(baseColumn);
        getColumns().add(offsetColumn);
        this.val1 = val1;
        this.val2 = val2;
    }

    @Override
    public boolean check(List<String> values) {
        Long date = Long.valueOf(values.get(0)) + Long.valueOf(values.get(1))* DateUtils.ONE_DAY;
        return  date <= val2 && date >= val1;
    }
}

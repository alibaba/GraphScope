package ldbc.snb.datagen.test.csv;

import java.io.File;
import java.util.Iterator;

public abstract class  Column <T> {

    protected Parser<T> parser = null;
    protected File file = null;
    protected int index = 0;
    protected int startIndex = 0;

    Column( Parser<T> parser, File file, int index, int startIndex ) {
        this.parser = parser;
        this.file = file;
        this.index = index;
        this.startIndex = startIndex;
    }


    public abstract Iterator<T> iterator();

}

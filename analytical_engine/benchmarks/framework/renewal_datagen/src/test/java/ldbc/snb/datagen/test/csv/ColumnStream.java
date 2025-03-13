package ldbc.snb.datagen.test.csv;


import java.io.File;
import java.util.Iterator;

public class ColumnStream<T> extends Column<T>{

    protected File file = null;
    protected int index = 0;

    public ColumnStream(Parser<T> parser, File file, int index, int startIndex) {
        super(parser, file, index, startIndex);
    }

    public static class ColumnStreamIterator<T> implements Iterator<T> {

        protected CsvFileReader reader = null;
        protected int index = 0;
        protected ColumnStream<T> columnStream = null;

        public ColumnStreamIterator( ColumnStream<T> columnStream, CsvFileReader reader, int index){
            this.reader = reader;
            this.index = index;
            this.columnStream = columnStream;
        }

        public void advance(int startIndex) {
            for(int i = 0; i < startIndex; ++i) {
                next();
            }
        }

        public boolean hasNext() {
            return reader.hasNext();
        }

        public T next() {
            String [] line = reader.next();
            return columnStream.parser.parse(line[index]);
        }

        public void remove() {
            // Intentionally left empty
        }
    }

    public Iterator<T> iterator( ) {
        try {
            CsvFileReader reader = new CsvFileReader(file);
            ColumnStreamIterator<T> iter =  new ColumnStreamIterator<>(this,reader,index);
            iter.advance(startIndex);
            return iter;
        } catch(Exception e) {
            System.err.println("Error opening csv reader");
        }
        return null;
    }
}

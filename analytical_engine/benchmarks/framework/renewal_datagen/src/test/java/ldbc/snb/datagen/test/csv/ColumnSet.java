package ldbc.snb.datagen.test.csv;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class ColumnSet<T> extends Column<T> {

    protected Set<T> data = null;

    public ColumnSet(Parser<T> parser, File file, int index, int startIndex) {
        super(parser, file, index, startIndex);
        data = new HashSet<>();
        try {
            CsvFileReader csvReader = new CsvFileReader(file);
            int count = 0;
            while(csvReader.hasNext()) {
                String[] line = csvReader.next();
                if(count >= startIndex ) {
                    data.add(this.parser.parse(line[index]));
                }
                count++;
            }
        } catch(Exception e) {
            System.err.println("Error while reading file");
            System.err.println(e.getMessage());
        }
    }


    public boolean contains(T element) {
        return data.contains(element);
    }

    public Iterator<T> iterator() {
        return data.iterator();
    }
}

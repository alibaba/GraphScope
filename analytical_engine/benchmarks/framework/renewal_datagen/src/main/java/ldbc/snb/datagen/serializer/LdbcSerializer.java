package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

abstract public class LdbcSerializer<TWriter extends HdfsWriter> implements Serializer<TWriter> {

    protected Map<FileName, TWriter> writers;

    abstract public List<FileName> getFileNames();

    abstract public void writeFileHeaders();

    public void initialize(Configuration conf, int reducerId) throws IOException {
        writers = initialize(conf, reducerId, isDynamic(), getFileNames());
        writeFileHeaders();
    }

    protected abstract boolean isDynamic();

    public void close() {
        for (FileName f : getFileNames()) {
            writers.get(f).close();
        }
    }

}

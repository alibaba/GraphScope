package ldbc.snb.datagen.hadoop.writer;


import java.io.IOException;
import java.util.List;

public class HdfsCsvWriter extends HdfsWriter {

    private String separator = "|";
    private StringBuffer buffer;
    private boolean endLineSeparator = true;

    public HdfsCsvWriter(String outputDir, String prefix, int numPartitions, boolean compressed, String separator, boolean endLineSeparator) throws IOException {
        super(outputDir, prefix, numPartitions, compressed, "csv");
        this.separator = separator;
        this.buffer = new StringBuffer(2048);
        this.endLineSeparator = endLineSeparator;

    }

    public void writeHeader(List<String> entry) {
        buffer.setLength(0);
        for (int i = 0; i < entry.size(); ++i) {
            buffer.append(entry.get(i));
            if ((endLineSeparator && i == (entry.size() - 1)) || (i < entry.size() - 1))
                buffer.append(separator);
        }
        buffer.append("\n");
        this.writeAllPartitions(buffer.toString());
    }

    public void writeEntry(List<String> entry) {
        buffer.setLength(0);
        for (int i = 0; i < entry.size(); ++i) {
            buffer.append(entry.get(i));
            if ((endLineSeparator && i == (entry.size() - 1)) || (i < entry.size() - 1))
                buffer.append(separator);
        }
        buffer.append("\n");
        this.write(buffer.toString());
    }
}

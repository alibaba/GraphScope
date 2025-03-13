package ldbc.snb.datagen.util.formatter;

import org.apache.hadoop.conf.Configuration;

public interface DateFormatter {
    void initialize(Configuration config);

    String formatDate(long date);

    String formatDateTime(long date);
}

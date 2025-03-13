package ldbc.snb.datagen.serializer.snb.csv;

import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.Serializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Map;

public interface CsvSerializer extends Serializer<HdfsCsvWriter> {

    default Map<FileName, HdfsCsvWriter> initialize(Configuration conf, int reducerId, boolean dynamic, List<FileName> fileNames) throws IOException {
        Map<FileName, HdfsCsvWriter> writers = new HashMap<>();
        for (FileName f : fileNames) {
            writers.put(f, new HdfsCsvWriter(
                    conf.get("ldbc.snb.datagen.serializer.socialNetworkDir") + (dynamic ? "/dynamic/" : "/static/"),
                    f.toString() + "_" + reducerId,
                    conf.getInt("ldbc.snb.datagen.serializer.numPartitions", 1),
                    conf.getBoolean("ldbc.snb.datagen.serializer.compressed", false), "|",
                    conf.getBoolean("ldbc.snb.datagen.serializer.endlineSeparator", false)
                )
            );
        }
        return writers;
    }

}

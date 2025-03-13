package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Serializer<THDFSWriter extends HdfsWriter> {

    Map<FileName, THDFSWriter> initialize(Configuration conf, int reducerId, boolean dynamic, List<FileName> fileNames) throws IOException;

}

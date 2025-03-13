

package ldbc.snb.datagen.serializer.graphalytics;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CsvDynamicPersonSerializerExtended extends DynamicPersonSerializer<HdfsCsvWriter> implements CsvSerializer {

    private HdfsCsvWriter[] writers;

    private enum FileNames {
        PERSON("person"),
        PERSON_KNOWS_PERSON("person_knows_person");

        private final String name;

        private FileNames(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }


    @Override
    public List<FileName> getFileNames() {
        return Collections.emptyList();
    }

    @Override
    public void writeFileHeaders() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {
        int numFiles = FileNames.values().length;
        writers = new HdfsCsvWriter[numFiles];
        for (int i = 0; i < numFiles; ++i) {
            writers[i] = new HdfsCsvWriter(conf.get("ldbc.snb.datagen.serializer.socialNetworkDir"), FileNames
                    .values()[i].toString() + "_" + reducerId, conf.getInt("ldbc.snb.datagen.serializer.numPartitions", 1), conf
                                                   .getBoolean("ldbc.snb.datagen.serializer.compressed", false), "|", conf
                                                   .getBoolean("ldbc.snb.datagen.serializer.endlineSeparator", false));
        }

        List<String> arguments = new ArrayList<>();
        arguments.add("id");
        arguments.add("creationDate");
        writers[FileNames.PERSON.ordinal()].writeHeader(arguments);

        arguments.clear();
        arguments.clear();
        arguments.add("Person.id");
        arguments.add("Person.id");
        arguments.add("CreationDate");
        arguments.add("Weight");
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeHeader(arguments);
    }

    @Override
    public void close() {
        int numFiles = FileNames.values().length;
        for (int i = 0; i < numFiles; ++i) {
            writers[i].close();
        }
    }

    @Override
    protected void serialize(Person p) {
        List<String> arguments = new ArrayList<>();
        arguments.add(Long.toString(p.accountId()));
        arguments.add(Dictionaries.dates.formatDateTime(p.creationDate()));
        writers[FileNames.PERSON.ordinal()].writeEntry(arguments);
    }

    @Override
    protected void serialize(StudyAt studyAt) {
        //Intentionally left empty
    }

    @Override
    protected void serialize(WorkAt workAt) {
        //Intentionally left empty
    }

    @Override
    protected void serialize(Person p, Knows knows) {
        List<String> arguments = new ArrayList<>();
        arguments.add(Long.toString(p.accountId()));
        arguments.add(Long.toString(knows.to().accountId()));
        arguments.add(Dictionaries.dates.formatDateTime(knows.creationDate()));
        arguments.add(Float.toString(knows.weight()));
        writers[FileNames.PERSON_KNOWS_PERSON.ordinal()].writeEntry(arguments);
    }

}

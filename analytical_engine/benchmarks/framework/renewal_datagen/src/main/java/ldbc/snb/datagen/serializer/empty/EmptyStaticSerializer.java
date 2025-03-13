package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmptyStaticSerializer extends StaticSerializer {

    @Override
    public List<FileName> getFileNames() {
        return Collections.emptyList();
    }

    @Override
    public void writeFileHeaders() {

    }

    @Override
    protected void serialize(final Place place) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final Organisation organisation) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final TagClass tagClass) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final Tag tag) {
        //Intentionally left empty

    }

    @Override
    public Map initialize(Configuration conf, int reducerId, boolean dynamic, List list) throws IOException {
        return Collections.emptyMap();
    }

}

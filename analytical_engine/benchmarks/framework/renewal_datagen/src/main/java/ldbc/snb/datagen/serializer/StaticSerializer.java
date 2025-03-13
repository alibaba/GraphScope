package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;

abstract public class StaticSerializer<TWriter extends HdfsWriter> extends LdbcSerializer<TWriter> {

    abstract protected void serialize(final Place place);

    abstract protected void serialize(final Organisation organisation);

    abstract protected void serialize(final TagClass tagClass);

    abstract protected void serialize(final Tag tag);

    public void export(final TagClass tagclass) {
        serialize(tagclass);
    }

    public void export(final Place place) {
        serialize(place);
    }

    public void export(final Organisation organisation) {
        serialize(organisation);
    }

    public void export(final Tag tag) {
        serialize(tag);
    }

    @Override
    protected boolean isDynamic() {
        return false;
    }

}

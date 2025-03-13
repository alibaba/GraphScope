package ldbc.snb.datagen.serializer.snb.csv.staticserializer;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.DBPOWL;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class CsvMergeForeignStaticSerializer extends StaticSerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(TAG, TAGCLASS, PLACE, ORGANISATION);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(TAG).writeHeader(ImmutableList.of("id", "name", "url", "hasType"));
        writers.get(TAGCLASS).writeHeader(ImmutableList.of("id", "name", "url", "isSubclassOf"));
        writers.get(PLACE).writeHeader(ImmutableList.of("id", "name", "url", "type", "isPartOf"));
        writers.get(ORGANISATION).writeHeader(ImmutableList.of("id", "type", "name", "url", "place"));
    }

    protected void serialize(final Place place) {
        writers.get(PLACE).writeEntry(ImmutableList.of(
            Integer.toString(place.getId()),
            place.getName(),
            DBP.getUrl(place.getName()),
            place.getType(),
            place.getType() == Place.CITY || place.getType() == Place.COUNTRY ? Integer.toString(Dictionaries.places.belongsTo(place.getId())) : ""
        ));
    }

    protected void serialize(final Organisation organisation) {
        writers.get(ORGANISATION).writeEntry(ImmutableList.of(
            Long.toString(organisation.id),
            organisation.type.toString(),
            organisation.name,
            DBP.getUrl(organisation.name),
            Integer.toString(organisation.location)
        ));
    }

    protected void serialize(final TagClass tagClass) {
        writers.get(TAGCLASS).writeEntry(ImmutableList.of(
            Integer.toString(tagClass.id),
            tagClass.name,
            tagClass.name.equals("Thing") ? "http://www.w3.org/2002/07/owl#Thing" : DBPOWL.getUrl(tagClass.name),
            tagClass.parent != -1 ? Integer.toString(tagClass.parent) : ""
        ));
    }

    protected void serialize(final Tag tag) {
        writers.get(TAG).writeEntry(ImmutableList.of(
            Integer.toString(tag.id),
            tag.name,
            DBP.getUrl(tag.name),
            Integer.toString(tag.tagClass)
        ));
    }

}

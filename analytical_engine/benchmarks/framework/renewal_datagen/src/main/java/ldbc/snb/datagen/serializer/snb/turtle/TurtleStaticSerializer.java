package ldbc.snb.datagen.serializer.snb.turtle;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.DBPOWL;
import ldbc.snb.datagen.vocabulary.FOAF;
import ldbc.snb.datagen.vocabulary.RDF;
import ldbc.snb.datagen.vocabulary.RDFS;
import ldbc.snb.datagen.vocabulary.SN;
import ldbc.snb.datagen.vocabulary.SNTAG;
import ldbc.snb.datagen.vocabulary.SNVOC;
import ldbc.snb.datagen.vocabulary.XSD;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class TurtleStaticSerializer extends StaticSerializer<HdfsWriter> implements TurtleSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_STATIC);
    }

    @Override
    public void writeFileHeaders() { }

    @Override
    protected void serialize(final Place place) {
        StringBuffer result = new StringBuffer(350);
        String name = place.getName();
        String type = DBPOWL.City;
        if (place.getType() == Place.COUNTRY) {
            type = DBPOWL.Country;
        } else if (place.getType() == Place.CONTINENT) {
            type = DBPOWL.Continent;
        }

        Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), DBP
                .fullPrefixed(name), RDF.type, DBPOWL.Place);
        Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), DBP.fullPrefixed(name), RDF.type, type);
        Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), DBP.fullPrefixed(name), FOAF.Name, Turtle
                .createLiteral(name));
        Turtle.createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.id,
                               Turtle.createDataTypeLiteral(Long.toString(place.getId()), XSD.Int));
        if (place.getType() != Place.CONTINENT) {
            String countryName = Dictionaries.places.getPlaceName(Dictionaries.places.belongsTo(place.getId()));
            Turtle.createTripleSPO(result, DBP.fullPrefixed(name), SNVOC.isPartOf, DBP.fullPrefixed(countryName));
            writers.get(SOCIAL_NETWORK_STATIC).write(result.toString());
        }
    }

    @Override
    protected void serialize(final Organisation organisation) {
        StringBuffer result = new StringBuffer(19000);
        if (organisation.type == Organisation.OrganisationType.company) {
            Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SN
                    .getCompURI(organisation.id), RDF.type, DBPOWL.Company);
            Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SN
                    .getCompURI(organisation.id), SNVOC.url, DBP.fullPrefixed(organisation.name));
            Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SN.getCompURI(organisation.id), FOAF.Name,
                                Turtle.createLiteral(organisation.name));
            Turtle.createTripleSPO(result, SN.getCompURI(organisation.id),
                                   SNVOC.locatedIn, DBP
                                           .fullPrefixed(Dictionaries.places.getPlaceName(organisation.location)));
            Turtle.createTripleSPO(result, SN.getCompURI(organisation.id), SNVOC.id,
                                   Turtle.createDataTypeLiteral(Long.toString(organisation.id), XSD.Int));
        } else {
            Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SN
                    .getUnivURI(organisation.id), RDF.type, DBPOWL.University);
            Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SN
                    .getUnivURI(organisation.id), SNVOC.url, DBP.fullPrefixed(organisation.name));
            Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SN.getUnivURI(organisation.id), FOAF.Name,
                                Turtle.createLiteral(organisation.name));
            Turtle.createTripleSPO(result, SN.getUnivURI(organisation.id),
                                   SNVOC.locatedIn, DBP
                                           .fullPrefixed(Dictionaries.places.getPlaceName(organisation.location)));
            Turtle.createTripleSPO(result, SN.getUnivURI(organisation.id), SNVOC.id,
                                   Turtle.createDataTypeLiteral(Long.toString(organisation.id), XSD.Int));
        }

        writers.get(SOCIAL_NETWORK_STATIC).write(result.toString());
    }

    @Override
    protected void serialize(final TagClass tagClass) {

        StringBuffer result = new StringBuffer(350);
        Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SN
                .getTagClassURI(tagClass.id), RDFS.label, Turtle
                                    .createLiteral(Dictionaries.tags.getClassName(tagClass.id)));
        Turtle.createTripleSPO(result, SN.getTagClassURI(tagClass.id), RDF.type, SNVOC.TagClass);

        if ("Thing".equals(tagClass.name)) {
            Turtle.createTripleSPO(result, SN
                    .getTagClassURI(tagClass.id), SNVOC.url, "<http://www.w3.org/2002/07/owl#Thing>");
        } else {
            Turtle.createTripleSPO(result, SN.getTagClassURI(tagClass.id), SNVOC.url, DBPOWL
                    .prefixed(Dictionaries.tags.getClassName(tagClass.id)));
        }

        Turtle.createTripleSPO(result, SN.getTagClassURI(tagClass.id), SNVOC.id,
                               Turtle.createDataTypeLiteral(Long.toString(tagClass.id), XSD.Int));
        writers.get(SOCIAL_NETWORK_STATIC).write(result.toString());

        Integer parent = Dictionaries.tags.getClassParent(tagClass.id);
        if (parent != -1) {
            Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SN
                    .getTagClassURI(tagClass.id), RDFS.subClassOf, SN.getTagClassURI(parent));
        }
    }

    @Override
    protected void serialize(final Tag tag) {
        StringBuffer result = new StringBuffer(350);
        Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SNTAG.fullPrefixed(tag.name), FOAF.Name, Turtle
                .createLiteral(tag.name));
        Integer tagClass = tag.tagClass;
        Turtle.writeDBPData(writers.get(SOCIAL_NETWORK_STATIC), SNTAG.fullPrefixed(tag.name), RDF.type, SN
                .getTagClassURI(tagClass));
        Turtle.createTripleSPO(result, SNTAG.fullPrefixed(tag.name), SNVOC.id,
                               Turtle.createDataTypeLiteral(Long.toString(tag.id), XSD.Int));
        writers.get(SOCIAL_NETWORK_STATIC).write(result.toString());
    }

}

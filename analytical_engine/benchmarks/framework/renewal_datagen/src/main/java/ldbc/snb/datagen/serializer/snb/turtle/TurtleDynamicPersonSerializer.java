

package ldbc.snb.datagen.serializer.snb.turtle;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.serializer.DynamicPersonSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.RDF;
import ldbc.snb.datagen.vocabulary.SN;
import ldbc.snb.datagen.vocabulary.SNTAG;
import ldbc.snb.datagen.vocabulary.SNVOC;
import ldbc.snb.datagen.vocabulary.XSD;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class TurtleDynamicPersonSerializer extends DynamicPersonSerializer<HdfsWriter> implements TurtleSerializer {

    private long workAtId = 0;
    private long studyAtId = 0;
    private long knowsId = 0;

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(SOCIAL_NETWORK_PERSON);
    }

    @Override
    public void writeFileHeaders() { }

    @Override
    protected void serialize(final Person p) {
        StringBuffer result = new StringBuffer(19000);
        String prefix = SN.getPersonURI(p.accountId());
        Turtle.addTriple(result, true, false, prefix, RDF.type, SNVOC.Person);
        Turtle.addTriple(result, false, false, prefix, SNVOC.id,
                         Turtle.createDataTypeLiteral(Long.toString(p.accountId()), XSD.Long));
        Turtle.addTriple(result, false, false, prefix, SNVOC.firstName,
                         Turtle.createLiteral(p.firstName()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.lastName,
                         Turtle.createLiteral(p.lastName()));

        if (p.gender() == 1) {
            Turtle.addTriple(result, false, false, prefix, SNVOC.gender,
                             Turtle.createLiteral("male"));
        } else {
            Turtle.addTriple(result, false, false, prefix, SNVOC.gender,
                             Turtle.createLiteral("female"));
        }
        Turtle.addTriple(result, false, false, prefix, SNVOC.birthday,
                         Turtle.createDataTypeLiteral(Dictionaries.dates.formatDate(p.birthday()), XSD.Date));
        Turtle.addTriple(result, false, false, prefix, SNVOC.ipaddress,
                         Turtle.createLiteral(p.ipAddress().toString()));
        Turtle.addTriple(result, false, false, prefix, SNVOC.browser,
                         Turtle.createLiteral(Dictionaries.browsers.getName(p.browserId())));
        Turtle.addTriple(result, false, true, prefix, SNVOC.creationDate,
                         Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(p.creationDate()), XSD.DateTime));

        Turtle.createTripleSPO(result, prefix, SNVOC.locatedIn, DBP
                .fullPrefixed(Dictionaries.places.getPlaceName(p.cityId())));

        for (Integer i : p.languages()) {
            Turtle.createTripleSPO(result, prefix, SNVOC.speaks,
                                   Turtle.createLiteral(Dictionaries.languages.getLanguageName(i)));
        }

        for (String email : p.emails()) {
            Turtle.createTripleSPO(result, prefix, SNVOC.email, Turtle.createLiteral(email));
        }

        for (Integer tag : p.interests()) {
            String interest = Dictionaries.tags.getName(tag);
            Turtle.createTripleSPO(result, prefix, SNVOC.hasInterest, SNTAG.fullPrefixed(interest));
        }
        writers.get(SOCIAL_NETWORK_PERSON).write(result.toString());
    }

    @Override
    protected void serialize(final StudyAt studyAt) {
        String prefix = SN.getPersonURI(studyAt.user);
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(studyAtId);
        Turtle.createTripleSPO(result, prefix, SNVOC.studyAt, SN.getStudyAtURI(id));
        Turtle.createTripleSPO(result, SN.getStudyAtURI(id), SNVOC.hasOrganisation,
                               SN.getUnivURI(studyAt.university));
        String yearString = Dictionaries.dates.formatYear(studyAt.year);
        Turtle.createTripleSPO(result, SN.getStudyAtURI(id), SNVOC.classYear,
                               Turtle.createDataTypeLiteral(yearString, XSD.Integer));
        studyAtId++;
        writers.get(SOCIAL_NETWORK_PERSON).write(result.toString());
    }

    @Override
    protected void serialize(final WorkAt workAt) {
        String prefix = SN.getPersonURI(workAt.user);
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(workAtId);
        Turtle.createTripleSPO(result, prefix, SNVOC.workAt, SN.getWorkAtURI(id));
        Turtle.createTripleSPO(result, SN.getWorkAtURI(id), SNVOC.hasOrganisation,
                               SN.getCompURI(workAt.company));
        String yearString = Dictionaries.dates.formatYear(workAt.year);
        Turtle.createTripleSPO(result, SN.getWorkAtURI(id), SNVOC.workFrom,
                               Turtle.createDataTypeLiteral(yearString, XSD.Integer));
        workAtId++;
        writers.get(SOCIAL_NETWORK_PERSON).write(result.toString());
    }

    @Override
    protected void serialize(final Person p, Knows knows) {
        String prefix = SN.getPersonURI(p.accountId());
        StringBuffer result = new StringBuffer(19000);
        long id = SN.formId(knowsId);
        Turtle.createTripleSPO(result, prefix, SNVOC.knows, SN.getKnowsURI(id));
        Turtle.createTripleSPO(result, SN.getKnowsURI(id), SNVOC.hasPerson,
                               SN.getPersonURI(knows.to().accountId()));

        Turtle.createTripleSPO(result, SN.getKnowsURI(id), SNVOC.creationDate,
                               Turtle.createDataTypeLiteral(TurtleDateTimeFormat.get().format(knows.creationDate()), XSD.DateTime));
        writers.get(SOCIAL_NETWORK_PERSON).write(result.toString());
        knowsId++;
    }

}

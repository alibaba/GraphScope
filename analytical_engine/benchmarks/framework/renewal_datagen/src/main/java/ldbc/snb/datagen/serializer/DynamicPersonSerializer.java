package ldbc.snb.datagen.serializer;

import com.google.common.base.Joiner;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.entities.dynamic.relations.StudyAt;
import ldbc.snb.datagen.entities.dynamic.relations.WorkAt;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

abstract public class DynamicPersonSerializer<TWriter extends HdfsWriter> extends LdbcSerializer<TWriter> {

    abstract protected void serialize(final Person p);

    abstract protected void serialize(final StudyAt studyAt);

    abstract protected void serialize(final WorkAt workAt);

    abstract protected void serialize(final Person p, final Knows knows);

    public void export(final Person person) {
        serialize(person);

        long universityId = Dictionaries.universities.getUniversityFromLocation(person.universityLocationId());
        if ((universityId != -1) && (person.classYear() != -1)) {
            StudyAt studyAt = new StudyAt();
            studyAt.year = person.classYear();
            studyAt.user = person.accountId();
            studyAt.university = universityId;
            serialize(studyAt);
        }

        Iterator<Long> it = person.companies().keySet().iterator();
        while (it.hasNext()) {
            long companyId = it.next();
            WorkAt workAt = new WorkAt();
            workAt.company = companyId;
            workAt.user = person.accountId();
            workAt.year = person.companies().get(companyId);
            serialize(workAt);
        }
    }

    public void export(final Person p, final Knows k) {
        if (p.accountId() < k.to().accountId())
            serialize(p, k);
    }

    public String getGender(int gender) {
        if (gender == 1) {
            return "male";
        } else {
            return "female";
        }
    }

    public String buildLanguages(List<Integer> languages) {
        return languages.stream()
                .map(l -> Dictionaries.languages.getLanguageName(l))
                .collect(Collectors.joining(";"));
    }

    public String buildEmail(TreeSet<String> emails) {
        return Joiner.on(";").join(emails);
    }

    @Override
    protected boolean isDynamic() {
        return true;
    }

}

package ldbc.snb.datagen.hadoop.serializer;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.LdbcDatagen;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class HadoopStaticSerializer {

    private StaticSerializer[] staticSerializer_;
    private TreeSet<Integer> exportedClasses_;
    private int currentFile_ = 0;

    private Configuration conf_;

    public HadoopStaticSerializer(Configuration conf) {
        conf_ = new Configuration(conf);
        exportedClasses_ = new TreeSet<>();
        LdbcDatagen.initializeContext(conf_);
    }

    public void run() throws Exception {

        try {
            staticSerializer_ = new StaticSerializer[DatagenParams.numThreads];
            for (int i = 0; i < DatagenParams.numThreads; ++i) {
                staticSerializer_[i] = (StaticSerializer) Class
                        .forName(conf_.get("ldbc.snb.datagen.serializer.staticSerializer")).newInstance();
                staticSerializer_[i].initialize(conf_, i);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }

        exportPlaces();
        exportTags();
        exportOrganizations();

        for (int i = 0; i < DatagenParams.numThreads; ++i) {
            staticSerializer_[i].close();
        }
    }

    private int nextFile() {
        int ret = currentFile_;
        currentFile_ = (++currentFile_) % DatagenParams.numThreads;
        return ret;
    }

    private void exportTagHierarchy(Tag tag) {
        int classId = tag.tagClass;
        while (classId != -1 && !exportedClasses_.contains(classId)) {
            exportedClasses_.add(classId);
            TagClass tagClass = new TagClass();
            tagClass.id = classId;
            tagClass.name = StringUtils.clampString(Dictionaries.tags.getClassName(classId), 256);
            tagClass.parent = Dictionaries.tags.getClassParent(tagClass.id);
            staticSerializer_[nextFile()].export(tagClass);
            classId = tagClass.parent;
        }
    }

    public void exportPlaces() {
        Set<Integer> locations = Dictionaries.places.getPlaces();
        Iterator<Integer> it = locations.iterator();
        while (it.hasNext()) {
            Place place = Dictionaries.places.getLocation(it.next());
            place.setName(StringUtils.clampString(place.getName(), 256));
            staticSerializer_[nextFile()].export(place);
        }
    }

    public void exportOrganizations() {
        Set<Long> companies = Dictionaries.companies.getCompanies();
        Iterator<Long> it = companies.iterator();
        while (it.hasNext()) {
            Organisation company = new Organisation();
            company.id = it.next();
            company.type = Organisation.OrganisationType.company;
            company.name = StringUtils.clampString(Dictionaries.companies.getCompanyName(company.id), 256);
            company.location = Dictionaries.companies.getCountry(company.id);
            staticSerializer_[nextFile()].export(company);
        }

        Set<Long> universities = Dictionaries.universities.getUniversities();
        it = universities.iterator();
        while (it.hasNext()) {
            Organisation university = new Organisation();
            university.id = it.next();
            university.type = Organisation.OrganisationType.university;
            university.name = StringUtils.clampString(Dictionaries.universities.getUniversityName(university.id), 256);
            university.location = Dictionaries.universities.getUniversityCity(university.id);
            staticSerializer_[nextFile()].export(university);
        }
    }

    public void exportTags() {
        Set<Integer> tags = Dictionaries.tags.getTags();
        Iterator<Integer> it = tags.iterator();
        while (it.hasNext()) {
            Tag tag = new Tag();
            tag.id = it.next();
            tag.name = StringUtils.clampString(Dictionaries.tags.getName(tag.id), 256);
            tag.name.replace("\"", "\\\"");
            tag.tagClass = Dictionaries.tags.getTagClass(tag.id);
            staticSerializer_[nextFile()].export(tag);
            exportTagHierarchy(tag);
        }
    }

}

package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.util.DateUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.GregorianCalendar;

public class Dictionaries {

    public static BrowserDictionary browsers = null;
    public static CompanyDictionary companies = null;
    public static DateUtils dates = null;
    public static EmailDictionary emails = null;
    public static IPAddressDictionary ips = null;
    public static LanguageDictionary languages = null;
    public static NamesDictionary names = null;
    public static PlaceDictionary places = null;
    public static PopularPlacesDictionary popularPlaces = null;
    public static TagDictionary tags = null;
    public static TagMatrix tagMatrix = null;
    public static TagTextDictionary tagText = null;
    public static UniversityDictionary universities = null;
    public static FlashmobTagDictionary flashmobs = null;


    public static void loadDictionaries(Configuration conf) {

        browsers = new BrowserDictionary(DatagenParams.probAnotherBrowser);

        dates = new DateUtils(conf, new GregorianCalendar(DatagenParams.startYear,
                                                              DatagenParams.startMonth,
                                                              DatagenParams.startDate),
                                  new GregorianCalendar(DatagenParams.endYear,
                                                        DatagenParams.endMonth,
                                                        DatagenParams.endDate),
                                  DatagenParams.alpha
        );


        emails = new EmailDictionary();

        places = new PlaceDictionary();

        ips = new IPAddressDictionary(places);


        languages = new LanguageDictionary(places,
                                           DatagenParams.probEnglish,
                                           DatagenParams.probSecondLang);

        names = new NamesDictionary(places);

        popularPlaces = new PopularPlacesDictionary(places);

        tags = new TagDictionary(places.getCountries().size(),
                                 DatagenParams.tagCountryCorrProb);

        tagMatrix = new TagMatrix();

        companies = new CompanyDictionary(places, DatagenParams.probUnCorrelatedCompany);

        universities = new UniversityDictionary(places,
                                                DatagenParams.probUnCorrelatedOrganization,
                                                DatagenParams.probTopUniv,
                                                companies.getNumCompanies());

        flashmobs = new FlashmobTagDictionary(tags,
                                              dates,
                                              DatagenParams.flashmobTagsPerMonth,
                                              DatagenParams.probInterestFlashmobTag,
                                              DatagenParams.probRandomPerLevel,
                                              DatagenParams.flashmobTagMinLevel,
                                              DatagenParams.flashmobTagMaxLevel,
                                              DatagenParams.flashmobTagDistExp);

        tagText = new TagTextDictionary(tags, DatagenParams.ratioReduceText);
    }

}

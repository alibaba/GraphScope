package ldbc.snb.datagen.vocabulary;

/**
 * RDF dbpedia ontology namespace used in the serialization process.
 */
public class DBPOWL {

    public static final String NAMESPACE = "http://dbpedia.org/ontology/";
    public static final String PREFIX = "dbpedia-owl:";

    public static final String Place = PREFIX + "Place";
    public static final String City = PREFIX + "City";
    public static final String Country = PREFIX + "Country";
    public static final String Continent = PREFIX + "Continent";
    public static final String Organisation = PREFIX + "Organisation";
    public static final String University = PREFIX + "University";
    public static final String Company = PREFIX + "Company";

    /**
     * Gets the dbpedia ontology prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the dbpedia ontology URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the dbpedia ontology RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}

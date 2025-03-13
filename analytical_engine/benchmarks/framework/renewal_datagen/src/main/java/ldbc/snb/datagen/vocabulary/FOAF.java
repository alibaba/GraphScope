package ldbc.snb.datagen.vocabulary;

/**
 * Friend of a friend project namespace used in the serialization process.
 */
public class FOAF {

    public static final String NAMESPACE = "http://xmlns.com/foaf/0.1/";
    public static final String PREFIX = "foaf:";

    public static final String Name = PREFIX + "name";

    /**
     * Gets the FOAF prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the FOAF URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the FOAF RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}

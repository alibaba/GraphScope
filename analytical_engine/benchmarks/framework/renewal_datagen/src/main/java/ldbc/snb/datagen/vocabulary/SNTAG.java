package ldbc.snb.datagen.vocabulary;

/**
 * LDBC social network vocabulary namespace used in the serialization process.
 */
public class SNTAG {

    public static final String NAMESPACE = "http://www.ldbc.eu/ldbc_socialnet/1.0/tag/";
    public static final String PREFIX = "sntag:";


    /**
     * Gets the LDBC social network vocabulary prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the LDBC social network vocabulary URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the LDBC social network vocabulary RDF-URL version of the input.
     */
    public static String fullPrefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}

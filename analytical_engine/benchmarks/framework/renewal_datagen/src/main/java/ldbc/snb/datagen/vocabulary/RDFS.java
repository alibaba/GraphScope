package ldbc.snb.datagen.vocabulary;

/**
 * RDF schema namespace used in the serialization process.
 */
public class RDFS {

    public static final String NAMESPACE = "http://www.w3.org/2000/01/rdf-schema#";
    public static final String PREFIX = "rdfs:";

    //Resources
    public static final String Datatype = PREFIX + "Datatype";
    public static final String Literal = PREFIX + "Literal";
    public static final String Resource = PREFIX + "Resource";

    //Properties
    public static final String comment = PREFIX + "comment";
    public static final String label = PREFIX + "label";
    public static final String subClassOf = PREFIX + "subClassOf";

    /**
     * Gets the RDF schema prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the RDF schema URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the RDF schema RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}

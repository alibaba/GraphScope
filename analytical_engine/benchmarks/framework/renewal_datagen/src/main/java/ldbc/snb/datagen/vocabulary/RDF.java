package ldbc.snb.datagen.vocabulary;

/**
 * RDF syntax namespace used in the serialization process.
 */
public class RDF {

    public static final String NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String PREFIX = "rdf:";

    //Resources
    public static final String Alt = PREFIX + "Alt";
    public static final String Bag = PREFIX + "Bag";
    public static final String Property = PREFIX + "Property";
    public static final String Seq = PREFIX + "Seq";
    public static final String Statement = PREFIX + "Statement";
    public static final String List = PREFIX + "List";
    public static final String nil = PREFIX + "nil";

    //Properties
    public static final String first = PREFIX + "first";
    public static final String rest = PREFIX + "rest";
    public static final String subject = PREFIX + "subject";
    public static final String predicate = PREFIX + "predicate";
    public static final String object = PREFIX + "object";
    public static final String type = PREFIX + "type";
    public static final String value = PREFIX + "value";

    /**
     * Gets the RDF syntax prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the RDF syntax URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the RDF syntax RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}

package ldbc.snb.datagen.vocabulary;

/**
 * XML schema namespace used in the serialization process.
 */
public class XSD {

    public static final String NAMESPACE = "http://www.w3.org/2001/XMLSchema#";
    public static final String PREFIX = "xsd:";

    //Resources
    public static final String Integer = PREFIX + "integer";
    public static final String Int = PREFIX + "int";
    public static final String Float = PREFIX + "float";
    public static final String Double = PREFIX + "double";
    public static final String Long = PREFIX + "long";
    public static final String String = PREFIX + "string";
    public static final String Decimal = PREFIX + "decimal";
    public static final String Year = PREFIX + "gYear";
    public static final String Date = PREFIX + "date";
    public static final String DateTime = PREFIX + "dateTime";


    /**
     * Gets the XML schema prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the XML schema URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the XML schema RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}

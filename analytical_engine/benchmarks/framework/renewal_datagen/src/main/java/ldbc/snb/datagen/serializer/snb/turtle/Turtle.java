package ldbc.snb.datagen.serializer.snb.turtle;

import ldbc.snb.datagen.hadoop.writer.HdfsWriter;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.DBPOWL;
import ldbc.snb.datagen.vocabulary.FOAF;
import ldbc.snb.datagen.vocabulary.RDF;
import ldbc.snb.datagen.vocabulary.RDFS;
import ldbc.snb.datagen.vocabulary.SN;
import ldbc.snb.datagen.vocabulary.SNTAG;
import ldbc.snb.datagen.vocabulary.SNVOC;
import ldbc.snb.datagen.vocabulary.XSD;

public class Turtle {

    public static String getNamespaces() {
        StringBuffer result = new StringBuffer(350);
        createPrefixLine(result, RDF.PREFIX, RDF.NAMESPACE);
        createPrefixLine(result, RDFS.PREFIX, RDFS.NAMESPACE);
        createPrefixLine(result, XSD.PREFIX, XSD.NAMESPACE);
        createPrefixLine(result, SNVOC.PREFIX, SNVOC.NAMESPACE);
        createPrefixLine(result, SNTAG.PREFIX, SNTAG.NAMESPACE);
        createPrefixLine(result, SN.PREFIX, SN.NAMESPACE);
        createPrefixLine(result, DBP.PREFIX, DBP.NAMESPACE);
        return result.toString();
    }

    /**
     * Gets the namespace for the static dbpedia file.
     */
    public static String getStaticNamespaces() {
        StringBuffer result = new StringBuffer(400);
        createPrefixLine(result, RDF.PREFIX, RDF.NAMESPACE);
        createPrefixLine(result, RDFS.PREFIX, RDFS.NAMESPACE);
        createPrefixLine(result, FOAF.PREFIX, FOAF.NAMESPACE);
        createPrefixLine(result, DBP.PREFIX, DBP.NAMESPACE);
        createPrefixLine(result, DBPOWL.PREFIX, DBPOWL.NAMESPACE);
        return result.toString();
    }

    /**
     * @param result:    The StringBuffer to append to.
     * @param prefix:    The RDF namespace prefix abbreviation.
     * @param namespace: The RDF namespace.
     */
    public static void createPrefixLine(StringBuffer result, String prefix, String namespace) {
        result.append("@prefix ");
        result.append(prefix);
        result.append(" ");
        result.append("<");
        result.append(namespace);
        result.append(">");
        result.append(" .\n");
    }

    /**
     * Writes a RDF triple in the dbpedia static data file.
     *
     * @param subject:   The RDF subject.
     * @param predicate: The RDF predicate.
     * @param object:    The RDF object.
     */
    public static void writeDBPData(HdfsWriter writer, String subject, String predicate, String object) {
        StringBuffer result = new StringBuffer(150);
        createTripleSPO(result, subject, predicate, object);
        writer.write(result.toString());
    }

    public static void writeDBPData(HdfsWriter writer, String data) {
        writer.write(data);
    }


    /**
     * Adds the appropriate triple kind into the input StringBuffer.
     *
     * @param result:    The StringBuffer to append to.
     * @param beginning: The beggining of a subject abbreviation block.
     * @param end:       The end of a subject abbreviation block.
     * @param subject:   The RDF subject.
     * @param predicate: The RDF predicate.
     */
    public static void addTriple(StringBuffer result, boolean beginning,
                                 boolean end, String subject, String predicate, String object1, String object2) {
        if (beginning) {
            result.append(subject + "\n");
        }

        if (object2.isEmpty()) {
            createTriplePO(result, predicate, object1, end);
        } else {
            createTriplePOO(result, predicate, object1, object2, end);
        }
    }

    /**
     * Adds the appropriate triple kind into the input StringBuffer.
     *
     * @param result:    The StringBuffer to append to.
     * @param beginning: The beggining of a subject abbreviation block.
     * @param end:       The end of a subject abbreviation block.
     * @param subject:   The RDF subject.
     * @param predicate: The RDF predicate.
     * @param object:    The RDF object.
     */
    public static void addTriple(StringBuffer result, boolean beginning,
                                 boolean end, String subject, String predicate, String object) {
        addTriple(result, beginning, end, subject, predicate, object, "");
    }


    /**
     * Builds a plain RDF literal.
     * <p>
     * See<a href="http://www.w3.org/TR/rdf-concepts/#section-Literals">RDF literals.</a>
     *
     * @param value: The value.
     * @return The RDF literal string representation.
     */
    public static String createLiteral(String value) {
        return "\"" + value + "\"";
    }

    /**
     * Builds a typed RDF literal.
     * <p>
     * See<a href="http://www.w3.org/TR/rdf-concepts/#section-Literals">RDF literals.</a>
     *
     * @param value:       The literal value.
     * @param datatypeURI: The data type.
     * @return The RDF typed literal string representation.
     */
    public static String createDataTypeLiteral(String value, String datatypeURI) {
        return "\"" + value + "\"^^" + datatypeURI;
    }

    /**
     * Builds a simple turtle triple: subject predicate object .
     * <p>
     * See <a href="http://www.w3.org/TeamSubmission/turtle/">Turtle</a>
     *
     * @param result:    The stringBuffer where the triple representation will be appended to.
     * @param subject:   The RDF subject.
     * @param predicate: The RDF predicate.
     * @param object:    The RDF object.
     */
    public static void createTripleSPO(StringBuffer result, String subject, String predicate, String object) {
        result.append(subject);
        result.append(" ");
        result.append(predicate);
        result.append(" ");
        result.append(object);
        result.append(" .\n");
    }

    /**
     * Builds a subject abbreviated turtle triple.
     * <p>
     * See <a href="http://www.w3.org/TeamSubmission/turtle/">Turtle</a>
     *
     * @param result:           The stringBuffer where the triple representation will be appended to.
     * @param predicate:        The RDF predicate.
     * @param object:           The RDF object.
     * @param endSubjectRepeat: The marker to end the subject repetition symbol.
     */
    public static void createTriplePO(StringBuffer result, String predicate, String object, boolean endSubjectRepeat) {
        result.append("    ");
        result.append(predicate);
        result.append(" ");
        result.append(object);
        if (endSubjectRepeat) {
            result.append(" .\n");
        } else {
            result.append(" ;\n");
        }
    }

    /**
     * Builds a subject abbreviated turtle triple with two objects.
     * <p>
     * See <a href="http://www.w3.org/TeamSubmission/turtle/">Turtle</a>
     *
     * @param result:           The stringBuffer where the triple representation will be appended to.
     * @param predicate:        The RDF predicate.
     * @param object1:          The first RDF object.
     * @param object2:          The second RDF object.
     * @param endSubjectRepeat: The marker to end the subject repetition symbol.
     */
    public static void createTriplePOO(StringBuffer result, String predicate, String object1, String object2, boolean endSubjectRepeat) {
        result.append("    ");
        result.append(predicate);
        result.append(" ");
        result.append(object1);
        result.append(" , ");
        result.append(object2);
        if (endSubjectRepeat) {
            result.append(" .\n");
        } else {
            result.append(" ;\n");
        }
    }
}

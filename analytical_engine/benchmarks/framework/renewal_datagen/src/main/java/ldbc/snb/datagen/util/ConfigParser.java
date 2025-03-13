package ldbc.snb.datagen.util;

import ldbc.snb.datagen.LdbcDatagen;
import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class ConfigParser {

    private static TreeMap<String, ScaleFactor> scaleFactors;
    private static final String SCALE_FACTORS_FILE = "scale_factors.xml";

    public static Configuration initialize() throws Exception {

        /** Default Parameters **/
        Configuration conf = new Configuration();
        conf.set("", Integer.toString(1));
        conf.set("ldbc.snb.datagen.generator.numPersons", "10000");
        conf.set("ldbc.snb.datagen.generator.startYear", "2010");
        conf.set("ldbc.snb.datagen.generator.numYears", "3");
        conf.set("ldbc.snb.datagen.generator.numThreads", Integer.toString(1));
        conf.set("ldbc.snb.datagen.serializer.dynamicActivitySerializer", "ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.activity.CSVBasicDynamicActivitySerializer");
        conf.set("ldbc.snb.datagen.serializer.dynamicPersonSerializer", "ldbc.snb.datagen.serializer.snb.csv.dynamicserializer.person.CSVBasicDynamicPersonSerializer");
        conf.set("ldbc.snb.datagen.serializer.staticSerializer", "ldbc.snb.datagen.serializer.snb.csv.staticserializer.CSVBasicStaticSerializer");
        conf.set("ldbc.snb.datagen.generator.distribution.degreeDistribution", "ldbc.snb.datagen.generator.distribution.FacebookDegreeDistribution");
        conf.set("ldbc.snb.datagen.generator.knowsGenerator", "ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceKnowsGenerator");
        conf.set("ldbc.snb.datagen.serializer.compressed", Boolean.toString(false));
        conf.set("ldbc.snb.datagen.serializer.updateStreams", Boolean.toString(true));
        conf.set("ldbc.snb.datagen.serializer.numPartitions", "1");
        conf.set("ldbc.snb.datagen.serializer.numUpdatePartitions", "1");
        conf.set("ldbc.snb.datagen.serializer.outputDir", "./");
        conf.set("ldbc.snb.datagen.serializer.socialNetworkDir", "./social_network");
        conf.set("ldbc.snb.datagen.serializer.hadoopDir", "./hadoop");
        conf.set("ldbc.snb.datagen.serializer.endlineSeparator", Boolean.toString(false));
        conf.set("ldbc.snb.datagen.generator.deltaTime", "10000");
        conf.set("ldbc.snb.datagen.generator.activity", "true");
        conf.set("ldbc.snb.datagen.serializer.dateFormatter", "ldbc.snb.datagen.util.formatter.StringDateFormatter");
        conf.set("ldbc.snb.datagen.util.formatter.StringDateFormatter.dateTimeFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        conf.set("ldbc.snb.datagen.util.formatter.StringDateFormatter.dateFormat", "yyyy-MM-dd");
        conf.set("ldbc.snb.datagen.generator.person.similarity", "ldbc.snb.datagen.entities.dynamic.person.similarity.GeoDistanceSimilarity");
        conf.set("ldbc.snb.datagen.parametergenerator.python", "python2");
        conf.set("ldbc.snb.datagen.parametergenerator.parameters", "true");
        conf.set("ldbc.snb.datagen.serializer.persons.sort", "true");

        /** Loading predefined Scale Factors **/

        try {
            scaleFactors = new TreeMap<>();
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(LdbcDatagen.class.getResourceAsStream("/" + SCALE_FACTORS_FILE));
            doc.getDocumentElement().normalize();

            System.out.println("Reading scale factors..");
            NodeList nodes = doc.getElementsByTagName("scale_factor");
            for (int i = 0; i < nodes.getLength(); i++) {
                Node node = nodes.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String scaleFactorName = element.getAttribute("name");
                    ScaleFactor scaleFactor = new ScaleFactor();
                    NodeList properties = ((Element) node).getElementsByTagName("property");
                    for (int j = 0; j < properties.getLength(); ++j) {
                        Element property = (Element) properties.item(j);
                        String name = property.getElementsByTagName("name").item(0).getTextContent();
                        String value = property.getElementsByTagName("value").item(0).getTextContent();
                        scaleFactor.properties.put(name, value);
                    }
                    System.out.println("Available scale factor configuration set " + scaleFactorName);
                    scaleFactors.put(scaleFactorName, scaleFactor);
                }
            }
            System.out.println("Number of scale factors read " + scaleFactors.size());
        } catch (ParserConfigurationException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        }
        return conf;
    }

    public static Configuration readConfig(Configuration conf, String paramsFile) {
        try {
            readConfig(conf, new FileInputStream(paramsFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return conf;
    }

    public static Configuration readConfig(Configuration conf, InputStream paramStream) {
        try {
            Properties properties = new Properties();
            properties.load(new InputStreamReader(paramStream, "UTF-8"));
            String val = (String) properties.get("ldbc.snb.datagen.generator.scaleFactor");
            if (val != null) {
                ScaleFactor scaleFactor = scaleFactors.get(val);
                System.out.println("Applied configuration of scale factor " + val);
                for (Map.Entry<String, String> e : scaleFactor.properties.entrySet()) {
                    conf.set(e.getKey(), e.getValue());
                }
            }

            for (String s : properties.stringPropertyNames()) {
                if (s.compareTo("ldbc.snb.datagen.generator.scaleFactor") != 0) {
                    conf.set(s, properties.getProperty(s));
                }
            }

            if (conf.get("fs.defaultFS").compareTo("file:///") == 0) {
                System.out.println("Running in standalone mode. Setting numThreads to 1");
                conf.set("ldbc.snb.datagen.generator.numThreads", "1");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
        return conf;
    }


    public static void printConfig(Configuration conf) {
        System.out.println("********* Configuration *********");
        Map<String, String> map = conf.getValByRegex("^(ldbc.snb.datagen).*$");
        for (Map.Entry<String, String> e : map.entrySet()) {
            System.out.println(e.getKey() + " " + e.getValue());
        }
        System.out.println("*********************************");
    }
}

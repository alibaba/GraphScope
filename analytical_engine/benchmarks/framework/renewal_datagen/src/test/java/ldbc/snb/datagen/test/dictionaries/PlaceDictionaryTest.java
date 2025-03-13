package ldbc.snb.datagen.test.dictionaries;

import ldbc.snb.datagen.LdbcDatagen;
import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.util.ConfigParser;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;
public class PlaceDictionaryTest {

    @Test
    public void testPopulationDistribution() {

            PlaceDictionary placeDictionary = new PlaceDictionary();
        try {
            Configuration conf = ConfigParser.initialize();
            ConfigParser.readConfig(conf, "./test_params.ini");
            ConfigParser.readConfig(conf, LdbcDatagen.class.getResourceAsStream("/params_default.ini"));
            LdbcDatagen.prepareConfiguration(conf);
            LdbcDatagen.initializeContext(conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int numPersons = 10000000;
        int countryFreqs[] = new int[placeDictionary.getCountries().size()];
        Arrays.fill(countryFreqs, 0);
        Random random = new Random(123456789);
        for (int i = 0; i < numPersons; ++i) {
            int nextCountry = placeDictionary.getCountryForUser(random);
            countryFreqs[nextCountry]++;
        }

        for( int i = 0; i < countryFreqs.length; ++i) {
            String countryName = placeDictionary.getPlaceName(i);
            int expectedPopulation = (int)(placeDictionary.getCumProbabilityCountry(i)*numPersons);
            int actualPopulation = countryFreqs[i];
            float error = Math.abs(expectedPopulation-actualPopulation)/(float)(expectedPopulation);
            assertTrue("Error in population of "+countryName+". Expected Population: "+expectedPopulation+", Actual " +
                               "Population: "+actualPopulation+". Error="+error,  error < 0.05) ;
        }
    }




}

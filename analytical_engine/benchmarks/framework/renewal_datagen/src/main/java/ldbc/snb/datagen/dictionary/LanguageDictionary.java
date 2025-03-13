package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;


public class LanguageDictionary {

    private static final String SEPARATOR = "  ";
    private static final String ISO_ENGLISH_CODE = "en";

    private List<String> languages;
    /**
     * < @brief The array of languages. *
     */
    private Map<Integer, List<Integer>> officalLanguagesByCountry;
    /**
     * < @brief The official languages by country. *
     */
    private Map<Integer, List<Integer>> languagesByCountry;
    /**
     * < @brief The languages by country. *
     */
    private PlaceDictionary placeDictionary;
    /**
     * < @brief The location dictionary. *
     */
    private double probEnglish;
    /**
     * < @brief The probability to speak english. *
     */
    private double probSecondLang;                 /**< @brief The probability of speaking a second language. **/

    /**
     * @param locationDic    The location dictionary.
     * @param probEnglish    The probability of speaking english.
     * @param probSecondLang The probability of speaking a second language.
     * @brief Constructor
     */
    public LanguageDictionary(PlaceDictionary locationDic,
                              double probEnglish, double probSecondLang) {
        this.placeDictionary = locationDic;
        this.probEnglish = probEnglish;
        this.probSecondLang = probSecondLang;
        this.languages = new ArrayList<>();
        this.officalLanguagesByCountry = new HashMap<>();
        this.languagesByCountry = new HashMap<>();
        load(DatagenParams.languageDictionaryFile);
    }

    /**
     * @param fileName The name of the dictionary file.
     * @brief Loads a dictionary file.
     */
    private void load(String fileName) {
        try {
            for (Integer id : placeDictionary.getCountries()) {
                officalLanguagesByCountry.put(id, new ArrayList<>());
                languagesByCountry.put(id, new ArrayList<>());
            }
            BufferedReader dictionary = new BufferedReader(new InputStreamReader(getClass()
                                                                                         .getResourceAsStream(fileName), "UTF-8"));
            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                if (placeDictionary.getCountryId(data[0]) != PlaceDictionary.INVALID_LOCATION) {
                    for (int i = 1; i < data.length; i++) {
                        Integer countryId = placeDictionary.getCountryId(data[0]);
                        String languageData[] = data[i].split(" ");
                        Integer id = languages.indexOf(languageData[0]);
                        if (id == -1) {
                            id = languages.size();
                            languages.add(languageData[0]);
                        }
                        if (languageData.length == 3) {
                            officalLanguagesByCountry.get(countryId).add(id);
                        }
                        languagesByCountry.get(countryId).add(id);
                    }
                }
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param languageId The language identifier.
     * @return The name of the language.
     * @brief Gets the name of the language.
     */
    public String getLanguageName(int languageId) {
        if (languageId < 0 || languageId >= languages.size()) {
            System.out.println("Trying to acces the invalid language with id=" + languageId);
            return "";
        }
        return languages.get(languageId);
    }

    /**
     * @param random  Random number generator.
     * @param country The country to retrieve the languages from.
     * @return The set of randomly choosen languages.
     * @breif Gets a set of random languages from a country.
     */
    public List<Integer> getLanguages(Random random, int country) {
        List<Integer> langSet = new ArrayList<>();
        if (officalLanguagesByCountry.get(country).size() != 0) {
            int id = random.nextInt(officalLanguagesByCountry.get(country).size());
            langSet.add(officalLanguagesByCountry.get(country).get(id));
        } else {
            int id = random.nextInt(languagesByCountry.get(country).size());
            langSet.add(languagesByCountry.get(country).get(id));
        }
        if (random.nextDouble() < probSecondLang) {
            int id = random.nextInt(languagesByCountry.get(country).size());
            if (langSet.indexOf(languagesByCountry.get(country).get(id)) == -1) {
                langSet.add(languagesByCountry.get(country).get(id));
            }
        }
        return langSet;
    }

    /**
     * @param random
     * @return The language.
     * @brief Gets a random language.
     */
    public int getInternationlLanguage(Random random) {
        Integer languageId = -1;
        if (random.nextDouble() < probEnglish) {
            languageId = languages.indexOf(ISO_ENGLISH_CODE);
        }
        return languageId;
    }
}

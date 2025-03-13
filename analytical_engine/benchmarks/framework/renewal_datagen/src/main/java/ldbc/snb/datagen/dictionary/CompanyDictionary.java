package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.util.RandomGeneratorFarm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class reads the file containing the names and countries for the companies used in the ldbc socialnet generation and
 * provides access methods to get such data.
 */
public class CompanyDictionary {

    private static final String SEPARATOR = "  ";
    private TreeMap<Long, String> companyName;
    /**
     * < @brief A map containing the name of each company.*
     */
    private TreeMap<Long, Integer> companyCountry;
    /**
     * < @brief A map containing the location of each company. *
     */
    private TreeMap<Integer, List<Long>> companiesByCountry;
    /**
     * < @brief A map containing the companies of each country. *
     */
    private PlaceDictionary placeDictionary;
    /**
     * < @brief The location dictionary.*
     */
    private double probUnCorrelatedCompany;    /**< @brief The probability of working in a uncorrelated company.**/

    /**
     * @param probUnCorrelatedCompany: Probability of selecting a country unrelated company.
     * @brief Constructor.
     */
    public CompanyDictionary(PlaceDictionary placeDictionary,
                             double probUnCorrelatedCompany) {

        this.placeDictionary = placeDictionary;
        this.probUnCorrelatedCompany = probUnCorrelatedCompany;
        this.companyName = new TreeMap<>();
        this.companyCountry = new TreeMap<>();
        this.companiesByCountry = new TreeMap<>();
        for (Integer id : placeDictionary.getCountries()) {
            this.companiesByCountry.put(id, new ArrayList<>());
        }
        load(DatagenParams.companiesDictionaryFile);
    }

    /**
     * @param fileName The file to load.
     * @brief Loads the company dictionary file.
     */
    private void load(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
            String line;
            int currentId = -1;
            long totalNumCompanies = 0;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                String locationName = data[0];
                String companyName = data[1].trim();
                if (placeDictionary.getCountryId(locationName) != PlaceDictionary.INVALID_LOCATION) {
                    currentId = placeDictionary.getCountryId(locationName);
                    companiesByCountry.get(currentId).add(totalNumCompanies);
                    companyCountry.put(totalNumCompanies, currentId);
                    this.companyName.put(totalNumCompanies, companyName);
                    totalNumCompanies++;
                }
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param company The company identifier.
     * @return The id of the country.
     * @brief Gets the company country id.
     */
    public int getCountry(Long company) {
        return companyCountry.get(company);
    }

    /**
     * @param randomFarm The random farm used to get random numbers.
     * @param countryId: A country id.
     * @return The id of the company.
     * @brief Gets a random company of the input country. In case the given country doesn't any company
     * a random one will be selected.
     */
    public long getRandomCompany(RandomGeneratorFarm randomFarm, int countryId) {
        int locId = countryId;
        List<Integer> countries = placeDictionary.getCountries();
        if (randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_COMPANY).nextDouble() <= probUnCorrelatedCompany) {
            locId = countries.get(randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_COMPANY_LOCATION)
                                            .nextInt(countries.size()));
        }
        // In case the country doesn't have any company select another country.
        while (companiesByCountry.get(locId).size() == 0) {
            locId = countries.get(randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_COMPANY_LOCATION)
                                            .nextInt(countries.size()));
        }
        int randomCompanyIdx = randomFarm.get(RandomGeneratorFarm.Aspect.COMPANY).nextInt(companiesByCountry.get(locId)
                                                                                                            .size());
        return companiesByCountry.get(locId).get(randomCompanyIdx);
    }


    /**
     * @return The set of companies.
     * @brief Gets the set of of companies in the dictionary.
     */
    public Set<Long> getCompanies() {
        return companyCountry.keySet();
    }

    /**
     * @return The number of companies.
     * @brief Gets the number of companies in the dictionary.
     */
    public int getNumCompanies() {
        return companyName.size();
    }

    /**
     * @param id The company's id.
     * @return The name of the company.
     * @brief Gets the name of a company.
     */
    public String getCompanyName(long id) {
        return companyName.get(id);
    }
}

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

public class UniversityDictionary {

    private static final String SEPARATOR = "  ";
    private TreeMap<Long, String> universityName;
    /**
     * < @brief The university names.
     */
    private TreeMap<Long, Integer> universityCity;
    /**
     * < @brief The university city.
     */
    private TreeMap<Integer, List<Long>> universitiesByCountry;
    /**
     * < @brief The universities by country .
     */
    private double probTopUniv;
    /**
     * < @brief The probability to get a top university.
     */
    private double probUncorrelatedUniversity;
    /**
     * < @brief The probability to get an uncorrelated university.
     */
    private PlaceDictionary locationDic;
    /**
     * < @brief The location dictionary.
     */
    private long startIndex = 0;             /**< @brief The first index to assign to university ids. */

    /**
     * @param locationDic                The location dictionary.
     * @param probUncorrelatedUniversity The probability to select an uncorrelated university.
     * @param probTopUni                 The probability of choosing a top university.
     * @param startIndex                 The first index to assign as id.
     * @brief Constructor
     */
    public UniversityDictionary(PlaceDictionary locationDic,
                                double probUncorrelatedUniversity,
                                double probTopUni, int startIndex) {
        this.probTopUniv = probTopUni;
        this.locationDic = locationDic;
        this.probUncorrelatedUniversity = probUncorrelatedUniversity;
        this.startIndex = startIndex;
        this.universityName = new TreeMap<>();
        this.universityCity = new TreeMap<>();
        this.universitiesByCountry = new TreeMap<>();
        for (Integer id : locationDic.getCountries()) {
            universitiesByCountry.put(id, new ArrayList<>());
        }
        load(DatagenParams.universityDictionaryFile);
    }

    /**
     * @param university The university id.
     * @return The university city.
     * @brief Get the location of the university.
     */
    public int getUniversityCity(long university) {
        return universityCity.get(university);
    }

    /**
     * @param fileName The universities file name.
     * @brief Loads a universities file.
     */
    private void load(String fileName) {
        try {
            BufferedReader dicAllInstitutes = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            String line;
            long totalNumUniversities = startIndex;
            while ((line = dicAllInstitutes.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                String countryName = data[0];
                String cityName = data[2];
                if (locationDic.getCountryId(countryName) != PlaceDictionary.INVALID_LOCATION &&
                        locationDic.getCityId(cityName) != PlaceDictionary.INVALID_LOCATION) {
                    int countryId = locationDic.getCountryId(countryName);
                    String universityName = data[1].trim();
                    universitiesByCountry.get(countryId).add(totalNumUniversities);
                    Integer cityId = locationDic.getCityId(cityName);
                    universityCity.put(totalNumUniversities, cityId);
                    this.universityName.put(totalNumUniversities, universityName);
                    totalNumUniversities++;
                }
            }
            dicAllInstitutes.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param randomFarm The random number generator farm.
     * @param countryId_ The country id.
     * @return The university id.
     * @brief Gets a random university.
     */
    public int getRandomUniversity(RandomGeneratorFarm randomFarm, int countryId_) {

        int countryId = countryId_;
        double prob = randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_UNIVERSITY).nextDouble();
        List<Integer> countries = locationDic.getCountries();
        if (randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_UNIVERSITY)
                      .nextDouble() <= probUncorrelatedUniversity) {
            countryId = countries.get(randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_UNIVERSITY_LOCATION)
                                                .nextInt(countries.size()));
        }

        while (universitiesByCountry.get(countryId).size() == 0) {
            countryId = countries.get(randomFarm.get(RandomGeneratorFarm.Aspect.UNCORRELATED_UNIVERSITY_LOCATION)
                                                .nextInt(countries.size()));
        }

        int range = universitiesByCountry.get(countryId).size();
        if (prob > probUncorrelatedUniversity && randomFarm.get(RandomGeneratorFarm.Aspect.TOP_UNIVERSITY)
                                                           .nextDouble() < probTopUniv) {
            range = Math.min(universitiesByCountry.get(countryId).size(), 10);
        }

        int randomUniversityIdx = randomFarm.get(RandomGeneratorFarm.Aspect.UNIVERSITY).nextInt(range);
        int zOrderLocation = locationDic.getZorderID(countryId);
        int universityLocation = (zOrderLocation << 24) | (randomUniversityIdx << 12);
        return universityLocation;
    }

    /**
     * @param universityLocation The encoded location.
     * @return The university id.
     * @brief Get the university from an encoded location.
     */
    public long getUniversityFromLocation(int universityLocation) {
        int zOrderLocationId = universityLocation >> 24;
        int universityId = (universityLocation >> 12) & 0x0FFF;
        int locationId = locationDic.getPlaceIdFromZOrder(zOrderLocationId);
        return universitiesByCountry.get(locationId).get(universityId);
    }

    /**
     * @param university The university id.
     * @return The name of the university.
     * @brief Gets the name of a university
     */
    public String getUniversityName(long university) {
        return universityName.get(university);
    }

    /**
     * @return The set of university ids.
     * @brief Gets all the university ids.
     */
    public Set<Long> getUniversities() {
        return universityCity.keySet();
    }
}

package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.util.ZOrder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * This class reads the files containing the country data and city data used in the ldbc socialnet generation and
 * provides access methods to get such data.
 * Most of the users has the prerequisite of requiring a valid location id.
 */
public class PlaceDictionary {

    public static final int INVALID_LOCATION = -1;
    private static final String SEPARATOR = " ";
    private static final String SEPARATOR_CITY = " ";

    private PlaceZOrder[] sortedPlace;
    private Float cumulativeDistribution[];

    private List<Integer> countries;
    /**
     * < @brief The set of countries. *
     */
    private Map<Integer, Place> places;
    /**
     * < @brief The places by id. *
     */
    private Map<Integer, Integer> isPartOf;
    /**
     * < @brief The location hierarchy. *
     */
    private Map<Integer, List<Integer>> citiesByCountry;
    /**
     * < @brief The cities by country. *
     */
    private Map<String, Integer> cityNames;
    /**
     * < @brief The city names. *
     */
    private Map<String, Integer> countryNames;       /**< @brief The country names. **/

    /**
     * Private class used to sort countries by their z-order value.
     */
    private class PlaceZOrder implements Comparable<PlaceZOrder> {

        public int id;
        public Integer zvalue;

        public PlaceZOrder(int id, int zvalue) {
            this.id = id;
            this.zvalue = zvalue;
        }

        public int compareTo(PlaceZOrder obj) {
            return zvalue.compareTo(obj.zvalue);
        }
    }

    /**
     * @brief Creator.
     */
    public PlaceDictionary() {
        this.countryNames = new HashMap<>();
        this.cityNames = new HashMap<>();
        this.places = new HashMap<>();
        this.isPartOf = new HashMap<>();
        this.countries = new ArrayList<>();
        this.citiesByCountry = new HashMap<>();
        load(DatagenParams.cityDictionaryFile, DatagenParams.countryDictionaryFile);
    }

    /**
     * @return The set of places.
     * @brief Gets the set of places.
     */
    public Set<Integer> getPlaces() {
        return places.keySet();
    }

    /**
     * @return The set of countries
     * @brief Gets a list of the country ids.
     */
    public List<Integer> getCountries() {
        return new ArrayList<>(countries);
    }

    /**
     * @param placeId Gets the name of a location.
     * @return The name of the location.
     * @brief Given a location id returns the name of said place.
     */
    public String getPlaceName(int placeId) {
        return places.get(placeId).getName();
    }

    /**
     * @param placeId The place identifier.
     * @return Population of that place.
     * @brief Given a place id returns the population of said place.
     */
    public Long getPopulation(int placeId) {
        return places.get(placeId).getPopulation();
    }

    /**
     * @param placeId The place identifier.
     * @return The type of the place.
     * @brief Gets the type of a place.
     */
    public String getType(int placeId) {
        return places.get(placeId).getType();
    }

    /**
     * @param placeId The place identifier.
     * @return The lattitude of the place.
     * @brief Gets the lattitude of a place.
     */
    public double getLatt(int placeId) {
        return places.get(placeId).getLatt();
    }

    /**
     * @param placeId The place identifier.
     * @return The longitude of the place.
     * @brief Gets the longitude of a place.
     */
    public double getLongt(int placeId) {
        return places.get(placeId).getLongt();
    }

    /**
     * @param cityName The name of the city.
     * @return The identifier of the city.
     * @brief Gets The identifier of a city.
     */
    public int getCityId(String cityName) {
        if (!cityNames.containsKey(cityName)) {
            return INVALID_LOCATION;
        }
        return cityNames.get(cityName);
    }

    /**
     * @param countryName The name of the country.
     * @return The identifier ot the country.
     * @brief Gets the identifier of a country.
     */
    public int getCountryId(String countryName) {
        if (!countryNames.containsKey(countryName)) {
            return INVALID_LOCATION;
        }
        return countryNames.get(countryName);
    }

    /**
     * @param placeId The place identifier.
     * @return The parent place identifier.
     * @brief Gets the parent of a place in the place hierarchy.
     */
    public int belongsTo(int placeId) {
        if (!isPartOf.containsKey(placeId)) {
            return INVALID_LOCATION;
        }
        return isPartOf.get(placeId);
    }

    /**
     * @param random    The random  number generator.
     * @param countryId The country Identifier.
     * @return The city identifier.
     * @brief Gets a random city from a country.
     */
    public int getRandomCity(Random random, int countryId) {
        if (!citiesByCountry.containsKey(countryId)) {
            System.out.println("Invalid countryId");
            return INVALID_LOCATION;
        }
        if (citiesByCountry.get(countryId).size() == 0) {
            Place placeId = places.get(countryId);
            String countryName = placeId.getName();
            System.out.println("Country with no known cities: " + countryName);
            return INVALID_LOCATION;
        }

        int randomNumber = random.nextInt(citiesByCountry.get(countryId).size());
        return citiesByCountry.get(countryId).get(randomNumber);
    }

    /**
     * @param random The random  number generator.
     * @return The country identifier.
     */
    public int getRandomCountryUniform(Random random) {
        int randomNumber = random.nextInt(countries.size());
        return countries.get(randomNumber);
    }

    /**
     * @param citiesFileName    The cities file name.
     * @param countriesFileName The countries file name.
     * @brief Loads the dictionary files.
     */
    private void load(String citiesFileName, String countriesFileName) {

        readCountries(countriesFileName);
        orderByZ();
        readCities(citiesFileName);
        readContinents(countriesFileName);
    }

    /**
     * @param fileName The cities file name to read.
     * @brief Reads a cities file name.
     */
    private void readCities(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR_CITY);
                if (countryNames.containsKey(data[0])) {
                    Integer countryId = countryNames.get(data[0]);
                    if (!cityNames.containsKey(data[1])) {
                        Place placeId = new Place();
                        placeId.setId(places.size());
                        placeId.setName(data[1]);
                        placeId.setLatt(places.get(countryId).getLatt());
                        placeId.setLongt(places.get(countryId).getLongt());
                        placeId.setPopulation(-1);
                        placeId.setType(Place.CITY);

                        places.put(placeId.getId(), placeId);
                        isPartOf.put(placeId.getId(), countryId);
                        citiesByCountry.get(countryId).add(placeId.getId());

                        cityNames.put(data[1], placeId.getId());
                    }
                }
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param fileName The countries file name.
     * @brief Reads a countries file.
     */
    private void readCountries(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            List<Float> temporalCumulative = new ArrayList<>();

            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                String placeName = data[1];

                Place place = new Place();
                place.setId(places.size());
                place.setName(placeName);
                place.setLatt(Double.parseDouble(data[2]));
                place.setLongt(Double.parseDouble(data[3]));
                place.setPopulation(Integer.parseInt(data[4]));
                place.setType(Place.COUNTRY);

                places.put(place.getId(), place);
                countryNames.put(placeName, place.getId());
                float dist = Float.parseFloat(data[5]);
                temporalCumulative.add(dist);
                countries.add(place.getId());

                citiesByCountry.put(place.getId(), new ArrayList<>());
            }
            dictionary.close();
            cumulativeDistribution = new Float[temporalCumulative.size()];
            cumulativeDistribution = temporalCumulative.toArray(cumulativeDistribution);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param fileName The continents file name.
     * @brief Reads a continents file name.
     */
    private void readContinents(String fileName) {
        Map<String, Integer> treatedContinents = new HashMap<>();
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            String line;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                String placeName = data[1];

                int countryId = countryNames.get(placeName);

                if (!treatedContinents.containsKey(data[0])) {

                    Place continent = new Place();
                    continent.setId(places.size());
                    continent.setName(data[0]);
                    continent.setLatt(Double.parseDouble(data[2]));
                    continent.setLongt(Double.parseDouble(data[3]));
                    continent.setPopulation(0);
                    continent.setType(Place.CONTINENT);

                    places.put(continent.getId(), continent);
                    treatedContinents.put(data[0], continent.getId());
                }
                Integer continentId = treatedContinents.get(data[0]);
                long population = places.get(continentId).getPopulation() + places.get(countryId).getPopulation();
                places.get(continentId).setPopulation(population);
                isPartOf.put(countryId, continentId);
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param random The random number generator.
     * @return The country for the user.
     * @brief Gets a country for a user.
     */
    public int getCountryForUser(Random random) {
        int position = Arrays.binarySearch(cumulativeDistribution, random.nextFloat());
        if (position >= 0) {
            return position;
        }
        return (-(position + 1));
    }

    public float getCumProbabilityCountry(int countryId) {
        if (countryId == 0) {
            return cumulativeDistribution[0];
        }
        return cumulativeDistribution[countryId] - cumulativeDistribution[countryId - 1];
    }

    /**
     * @brief Sorts places by Z order.
     */
    private void orderByZ() {
        ZOrder zorder = new ZOrder(8);
        sortedPlace = new PlaceZOrder[countries.size()];

        for (int i = 0; i < countries.size(); i++) {
            Place loc = places.get(countries.get(i));
            int zvalue = zorder.getZValue(((int) Math.round(loc.getLongt()) + 180) / 2, ((int) Math
                    .round(loc.getLatt()) + 180) / 2);
            sortedPlace[i] = new PlaceZOrder(loc.getId(), zvalue);
        }

        Arrays.sort(sortedPlace);
        for (int i = 0; i < sortedPlace.length; i++) {
            places.get(sortedPlace[i].id).setzId(i);
        }
    }

    /**
     * @param placeId The place identifier.
     * @return The z order of the place.
     * @brief Gets the z order of a place.
     */
    public int getZorderID(int placeId) {
        return places.get(placeId).getzId();
    }

    /**
     * @param zOrderId the z order.
     * @return The place identifier.
     * @brief Gets the place identifier from a z order.
     */
    public int getPlaceIdFromZOrder(int zOrderId) {
        return sortedPlace[zOrderId].id;
    }

    /**
     * @param id The place identifier.
     * @return The place whose identifier is id.
     * @brief Gets a place from its identifier.
     */
    public Place getLocation(int id) {
        return places.get(id);
    }
}

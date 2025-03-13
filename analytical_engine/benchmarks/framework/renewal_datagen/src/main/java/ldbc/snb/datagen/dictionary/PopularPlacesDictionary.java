package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.statictype.place.PopularPlace;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;


public class PopularPlacesDictionary {

    private PlaceDictionary placeDictionary;
    /**
     * < @brief The location dictionary. *
     */
    private Map<Integer, List<PopularPlace>> popularPlacesByCountry;   /**< @brief The popular places by country .**/

    /**
     * @param locationDic The location dictionary.
     * @brief Constructor
     */
    public PopularPlacesDictionary(PlaceDictionary locationDic) {
        this.placeDictionary = locationDic;
        this.popularPlacesByCountry = new HashMap<>();
        for (Integer id : placeDictionary.getCountries()) {
            this.popularPlacesByCountry.put(id, new ArrayList<>());
        }
        load(DatagenParams.popularDictionaryFile);
    }

    /**
     * @param fileName The popular places file name.
     * @brief Loads a popular places file.
     */
    private void load(String fileName) {
        String line;
        String locationName;
        String lastLocationName = "";
        int curLocationId = -1;

        String label;
        try {
            BufferedReader dicPopularPlace = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

            while ((line = dicPopularPlace.readLine()) != null) {
                double latt;
                double longt;
                String infos[] = line.split("  ");
                locationName = infos[0];
                if (locationName.compareTo(lastLocationName) != 0) {
                    if (placeDictionary.getCountryId(locationName) != PlaceDictionary.INVALID_LOCATION) {
                        lastLocationName = locationName;
                        curLocationId = placeDictionary.getCountryId(locationName);
                        label = infos[2];
                        latt = Double.parseDouble(infos[3]);
                        longt = Double.parseDouble(infos[4]);
                        popularPlacesByCountry.get(curLocationId).add(new PopularPlace(label, latt, longt));
                    }
                } else {
                    label = infos[2];
                    latt = Double.parseDouble(infos[3]);
                    longt = Double.parseDouble(infos[4]);
                    popularPlacesByCountry.get(curLocationId).add(new PopularPlace(label, latt, longt));
                }
            }
            dicPopularPlace.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param random    The random number generator.
     * @param countryId the locationid
     * @return The popular place identifier.
     * @brief Gets the popular places of a country.
     */
    public short getPopularPlace(Random random, int countryId) {
        if (popularPlacesByCountry.get(countryId).size() == 0) {
            return -1;
        }
        return (short) random.nextInt(popularPlacesByCountry.get(countryId).size());
    }

    /**
     * @param countryId the id of the country.
     * @param placeId   The popular place id.
     * @return The popular place.
     * @brief Gets a popular place.
     */
    public PopularPlace getPopularPlace(int countryId, int placeId) {
        return popularPlacesByCountry.get(countryId).get(placeId);
    }
}

package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;
import umontreal.iro.lecuyer.probdist.GeometricDist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

public class NamesDictionary {

    /**
     * Geometric probability used
     */
    private static final double GEOMETRIC_RATIO = 0.2;

    private static final int topN = 30;

    private PlaceDictionary locationDic;
    private Map<Integer, Vector<String>> surNamesByLocations;
    private Vector<Map<Integer, Vector<String>>> givenNamesByLocationsMale;    // Year / Location / Names
    private Vector<Map<Integer, Vector<String>>> givenNamesByLocationsFemale;
    private GeometricDist geoDist;

    public NamesDictionary(PlaceDictionary locationDic) {
        this.locationDic = locationDic;
        geoDist = new GeometricDist(GEOMETRIC_RATIO);
        init();
    }

    private void init() {
        surNamesByLocations = new HashMap<>();
        for (Integer id : locationDic.getCountries()) {
            surNamesByLocations.put(id, new Vector<>());
        }

        //assume that there is only 2 periods of birthyears
        int birthYearPeriod = 2;
        givenNamesByLocationsMale = new Vector<>(birthYearPeriod);
        givenNamesByLocationsFemale = new Vector<>(birthYearPeriod);
        for (int i = 0; i < birthYearPeriod; i++) {
            givenNamesByLocationsMale.add(new HashMap<>());
            givenNamesByLocationsFemale.add(new HashMap<>());
            for (Integer id : locationDic.getCountries()) {
                givenNamesByLocationsMale.lastElement().put(id, new Vector<>());
                givenNamesByLocationsFemale.lastElement().put(id, new Vector<>());
            }
        }

        extractSurNames();
        extractGivenNames();
    }

    public void extractSurNames() {
        try {
            BufferedReader surnameDictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(DatagenParams.surnamDictionaryFile), "UTF-8"));

            String line;
            int totalSurNames = 0;
            while ((line = surnameDictionary.readLine()) != null) {
                String infos[] = line.split(",");
                String locationName = infos[1];
                int locationId = locationDic.getCountryId(locationName);
                if (locationId != locationDic.INVALID_LOCATION) {
                    String surName = infos[2].trim();
                    surNamesByLocations.get(locationId).add(surName);
                    totalSurNames++;
                }
            }
            surnameDictionary.close();
            System.out.println("Done ... " + totalSurNames + " surnames were extracted ");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void extractGivenNames() {
        try {
            BufferedReader givennameDictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(DatagenParams.nameDictionaryFile), "UTF-8"));

            String line;
            int totalGivenNames = 0;
            while ((line = givennameDictionary.readLine()) != null) {
                String infos[] = line.split("  ");
                String locationName = infos[0];
                int gender = Integer.parseInt(infos[2]);
                int birthYearPeriod = Integer.parseInt(infos[3]);
                int locationId = locationDic.getCountryId(locationName);
                if (locationId != locationDic.INVALID_LOCATION) {
                    String givenName = infos[1].trim();
                    if (gender == 0) {
                        givenNamesByLocationsMale.get(birthYearPeriod).get(locationId).add(givenName);
                    } else {
                        givenNamesByLocationsFemale.get(birthYearPeriod).get(locationId).add(givenName);
                    }
                    totalGivenNames++;
                }
            }
            givennameDictionary.close();
            System.out.println("Done ... " + totalGivenNames + " given names were extracted ");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /*
     * If the number of names is smaller than the computed rank
     * uniformly get a name from all names
     * Else, from 0 to (limitRank - 1) will be distributed according to
     * geometric distribution, out of this scope will be distribution
     */
    private int getGeoDistRandomIdx(Random random, int numNames) {
        int nameIdx = -1;
        double prob = random.nextDouble();
        int rank = geoDist.inverseFInt(prob);

        if (rank < topN) {
            if (numNames > rank) {
                nameIdx = rank;
            } else {
                nameIdx = random.nextInt(numNames);
            }
        } else {
            if (numNames > rank) {
                nameIdx = topN + random.nextInt(numNames - topN);
            } else {
                nameIdx = random.nextInt(numNames);
            }
        }

        return nameIdx;
    }

    public String getRandomSurname(Random random, int locationId) {
        int surNameIdx = getGeoDistRandomIdx(random, surNamesByLocations.get(locationId).size());
        return surNamesByLocations.get(locationId).get(surNameIdx);
    }

    public String getRandomGivenName(Random random, int locationId, boolean isMale, int birthYear) {
        String name = "";
        int period = (birthYear < 1985) ? 0 : 1;
        Vector<Map<Integer, Vector<String>>> target = (isMale) ? givenNamesByLocationsMale : givenNamesByLocationsFemale;

        // Note that, only vector of names for the first period contains list of names not in topN
        int nameId = getGeoDistRandomIdx(random, target.get(0).get(locationId).size());
        if (nameId >= topN) {
            name = target.get(0).get(locationId).get(nameId);
        } else {
            name = target.get(period).get(locationId).get(nameId);
        }

        return name;
    }

    /**
     * return a given name which is the median of topN for a given location/gender/year
     * we use it for parameter generation
     */
    public String getMedianGivenName(int locationId, boolean isMale, int birthYear) {
        int period = 0;
        Vector<Map<Integer, Vector<String>>> target = (isMale) ? givenNamesByLocationsMale : givenNamesByLocationsFemale;
        int size = target.get(period).get(locationId).size();
        String name = target.get(period).get(locationId).get(size / 2);
        return name;
    }
}


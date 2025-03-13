package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.IP;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;


public class IPAddressDictionary {

    private static final String SEPARATOR_COUNTRY = "   ";
    private static final String SEPARATOR_IP = "[.]";
    private static final String SEPARATOR_MASK = "/";
    private static final int MAX_IP_COUNTRY = 100;
    private TreeMap<Integer, List<IP>> ipsByCountry;
    /**
     * < @brief The country of ips. *
     */
    private PlaceDictionary placeDictionary;

    /**
     * < @brief The location dictionary. *
     */

    public IPAddressDictionary(PlaceDictionary locationDic) {

        this.placeDictionary = locationDic;
        this.ipsByCountry = new TreeMap<>();
        load(DatagenParams.countryAbbrMappingFile, DatagenParams.IPZONE_DIRECTORY);
    }

    /**
     * @param mappingFileName The abbreviations per country.
     * @param baseIPdir       The base directory where ip files are found.
     * @breif Loads dictionary.
     */
    private void load(String mappingFileName, String baseIPdir) {
        String line;
        Map<String, String> countryAbbreMap = new HashMap<>();
        try {
            BufferedReader mappingFile = new BufferedReader(new InputStreamReader(getClass()
                                                                                          .getResourceAsStream(mappingFileName), "UTF-8"));
            while ((line = mappingFile.readLine()) != null) {
                String data[] = line.split(SEPARATOR_COUNTRY);
                String abbr = data[0];
                String countryName = data[1].trim().replace(" ", "_");
                countryAbbreMap.put(countryName, abbr);
            }
            mappingFile.close();

            List<Integer> countries = placeDictionary.getCountries();
            for (int i = 0; i < countries.size(); i++) {
                ipsByCountry.put(countries.get(i), new ArrayList<>());

                //Get the name of file
                String fileName = countryAbbreMap.get(placeDictionary.getPlaceName(countries.get(i)));
                fileName = baseIPdir + "/" + fileName + ".zone";
                BufferedReader ipZoneFile = new BufferedReader(new InputStreamReader(getClass()
                                                                                             .getResourceAsStream(fileName), "UTF-8"));

                int j = 0;
                while ((line = ipZoneFile.readLine()) != null && (j < MAX_IP_COUNTRY)) {
                    String data[] = line.split(SEPARATOR_IP);
                    String maskData[] = data[3].split(SEPARATOR_MASK);
                    int byte1 = Integer.valueOf(data[0]);
                    int byte2 = Integer.valueOf(data[1]);
                    int byte3 = Integer.valueOf(data[2]);
                    int byte4 = Integer.valueOf(maskData[0]);
                    int maskNum = Integer.valueOf(maskData[1]);

                    IP ip = new IP(byte1, byte2, byte3, byte4, maskNum);

                    ipsByCountry.get(i).add(ip);
                    j++;
                }
                ipZoneFile.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public IP getIP(Random random, int countryId) {
        int finalLocationIndex = countryId;
        while (!placeDictionary.getType(finalLocationIndex).equals("country")) {
            finalLocationIndex = placeDictionary.belongsTo(finalLocationIndex);
        }
        List<IP> countryIPs = ipsByCountry.get(finalLocationIndex);
        int idx = random.nextInt(countryIPs.size());

        IP networkIp = countryIPs.get(idx);

        int mask = networkIp.getMask();
        int network = networkIp.getNetwork();

        IP newIp = new IP(network | ((~mask) & random.nextInt()), mask);

        return newIp;
    }

}

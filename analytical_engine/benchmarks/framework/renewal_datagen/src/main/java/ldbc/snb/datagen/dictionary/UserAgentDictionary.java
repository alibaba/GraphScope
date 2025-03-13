package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class UserAgentDictionary {

    private List<String> userAgents;
    /**
     * < @brief The set of all user agents.
     */
    private double probSentFromAgent;      /**< @brief The probability to used a different agent.*/

    /**
     * @param probSentFromAgent The probability to use a different agent.
     * @brief Constructor
     */
    public UserAgentDictionary(double probSentFromAgent) {
        this.probSentFromAgent = probSentFromAgent;
        load(DatagenParams.agentFile);
    }

    /**
     * @param fileName The agent dictionary file name.
     * @brief Loads an agent dictionary file.
     */
    private void load(String fileName) {
        try {
            userAgents = new ArrayList<>();
            BufferedReader agentFile = new BufferedReader(new InputStreamReader(getClass()
                                                                                        .getResourceAsStream(fileName), "UTF-8"));
            String line;
            while ((line = agentFile.readLine()) != null) {
                userAgents.add(line.trim());
            }
            agentFile.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param randomSent    The random number generator.
     * @param hasSmathPhone Tells if we want an smartphone.
     * @param agentId       The user agent id.
     * @return The user agent name.
     * @brief Get a user agen name.
     */
    public String getUserAgentName(Random randomSent, boolean hasSmathPhone, int agentId) {
        return (hasSmathPhone && (randomSent.nextDouble() > probSentFromAgent)) ? userAgents.get(agentId) : "";
    }

    /**
     * @param random The random number generator used.
     * @return The user agent id.
     * @brief Gets a random user agent.
     */
    public int getRandomUserAgentIdx(Random random) {
        return random.nextInt(userAgents.size());
    }
}

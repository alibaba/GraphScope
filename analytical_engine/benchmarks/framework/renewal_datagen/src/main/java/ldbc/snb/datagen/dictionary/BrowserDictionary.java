package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.DatagenParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class BrowserDictionary {

    private static final String SEPARATOR_ = "  ";
    private List<String> browsers_;
    private List<Double> cumulativeDistribution_;
    private double probAnotherBrowser_ = 0.0f;

    public BrowserDictionary(double probAnotherBrowser) {
        probAnotherBrowser_ = probAnotherBrowser;
        browsers_ = new ArrayList<>();
        cumulativeDistribution_ = new ArrayList<>();
        load(DatagenParams.browserDictonryFile);
    }

    private void load(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
            String line;
            double cummulativeDist = 0.0;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR_);
                String browser = data[0];
                cummulativeDist += Double.parseDouble(data[1]);
                browsers_.add(browser);
                cumulativeDistribution_.add(cummulativeDist);
            }
            dictionary.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName(int id) {
        return browsers_.get(id);
    }

    public int getRandomBrowserId(Random random) {
        double prob = random.nextDouble();
        int minIdx = 0;
        int maxIdx = (byte) ((prob < cumulativeDistribution_.get(minIdx)) ? minIdx : cumulativeDistribution_
                .size() - 1);
        // Binary search
        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution_.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return maxIdx;
    }

    public int getPostBrowserId(Random randomDiffBrowser, Random randomBrowser, int userBrowserId) {
        double prob = randomDiffBrowser.nextDouble();
        return (prob < probAnotherBrowser_) ? getRandomBrowserId(randomBrowser) : userBrowserId;
    }
}

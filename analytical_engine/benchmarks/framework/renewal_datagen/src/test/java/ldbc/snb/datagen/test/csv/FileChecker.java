package ldbc.snb.datagen.test.csv;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileChecker {

    private String fileName = null;
    private List<Check> checks = null;

    public FileChecker(String fileName) {
        this.fileName = fileName;
        this.checks = new ArrayList<>();
    }

    public void addCheck( Check check) {
        checks.add(check);
    }

    public boolean run(int startLine) {
        File file = new File(fileName);
        try {
            CsvFileReader csvReader = new CsvFileReader(file);
            int lineCount = 1;
            while(csvReader.hasNext()) {
                String[] line = csvReader.next();
                if(startLine <= lineCount-1) {
                    for (Check c : checks) {
                        List<String> row = new ArrayList<>();
                        for (Integer index : c.getColumns()) {
                            row.add(line[index]);
                        }
                        if (!c.check(row)) {
                            System.err.print("Found error at file " + fileName + " at line " + lineCount);
                            System.err.print(" when applying " + c.getCheckName()+" on columns ");
                            for(Integer index : c.getColumns()) {
                               System.err.print(index+" ");
                            }
                            System.err.print(" with values ");
                            for(String index : row) {
                                System.err.print(index+" ");
                            }
                            System.err.println();
                            return false;
                        }
                    }
                }
                lineCount++;
            }
        }catch(Exception e) {
            System.err.println(e.getMessage());
            return false;
        }
        return true;
    }
}

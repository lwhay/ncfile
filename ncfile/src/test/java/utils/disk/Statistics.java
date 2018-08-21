/**
 * 
 */
package utils.disk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Michael
 *
 */
public class Statistics {
    private static final String path = "src/resources/disk/runtime.log";
    private static final String type = "8";

    private static String lineHint = "";
    private static String lineTime = "processed";
    private static String lineRead = "";
    private static List<String> lineThreads = new ArrayList<>();
    private static List<String> readThreads = new ArrayList<>();

    private static void handleResult() {
        double ranges = .0;
        for (String tline : lineThreads) {
            String[] fields = tline.split(" ");
            int r = Integer.parseInt(fields[fields.length - 1]);
            ranges += r;
        }
        ranges /= lineThreads.size();
        double skipps = .0;
        double ecount = .0;
        long rcount = 0;
        for (String tline : readThreads) {
            String[] fields = tline.split(" ");
            if (fields[fields.length - 2].startsWith("skipping")) {
                double r = .0;
                int c = 0;
                switch (type) {
                    case "2":
                    case "5":
                        c = Integer.parseInt(fields[2]);
                        r = Double.parseDouble(fields[fields.length - 1]);
                        break;
                    case "8":
                        if (fields.length != 12) {
                            continue;
                        }
                        r = Double.parseDouble(fields[fields.length - 1]);
                        try {
                            c = Integer.parseInt(fields[5]);
                        } catch (Exception e) {
                            System.out.println(lineHint);
                        }
                        break;
                }
                skipps += r;
                rcount += c;
                ecount++;
            }
        }
        skipps /= ecount;
        System.out.println(lineHint + " " + lineRead + " range: " + ranges + " read: " + rcount + " skipps: " + skipps);
        lineHint = "";
        lineTime = "processed";
        lineThreads.clear();
        readThreads.clear();
    }

    private static void sequentialStat() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path));
        while ((lineRead = br.readLine()) != null) {
            if (lineRead.startsWith("Total runtime")) {
                if (lineHint.startsWith("./test.sh " + type) && lineTime.equals("inprocess")) {
                    handleResult();
                }
            } else if (lineRead.startsWith("./test.sh " + type)) {
                if (lineTime.startsWith("./test.sh " + type)) {
                    handleResult();
                } else {
                    lineHint = lineRead;
                    //lineTime = "inprocess";
                }
            } else if (lineRead.startsWith("type: " + type)) {
                if (lineHint.startsWith("./test.sh " + type) && lineTime.equals("inprocess")) {
                    lineThreads.add(lineRead);
                }
            } else if (lineRead.startsWith("Read count")) {
                if (lineHint.startsWith("./test.sh " + type) && lineTime.equals("inprocess")) {
                    readThreads.add(lineRead);
                }
            } else if (lineRead.startsWith("rm")) {
                if (lineHint.startsWith("./test.sh " + type)) {
                    lineTime = "inprocess";
                }
            }
        }
        br.close();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        switch (type) {
            case "2":
            case "5":
            case "8":
                sequentialStat();
                break;
            default:
                break;
        }
    }

}

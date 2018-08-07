/**
 * 
 */
package neci.parallel;

import java.io.IOException;

import neci.parallel.worker.FilteringScanner;
import neci.parallel.worker.tpch.Q03_Worker;
import neci.parallel.worker.tpch.Q10_Worker;
import neci.parallel.worker.tpch.Q15_Worker;

/**
 * @author Michael
 *
 */
public class MultiThreadTPCHFilterScanTest {

    /**
     * @param args
     * @throws IOException
     * @throws NumberFormatException
     * @throws InterruptedException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws NumberFormatException, IOException, InstantiationException,
            IllegalAccessException, InterruptedException {
        if (args.length != 7) {
            System.out.println(
                    "Command: String schemaPath, String targetPath, String jsonquery, int degree, int batchSize, int blockSize, String qid");
            System.exit(0);
        }
        long begin = System.currentTimeMillis();
        MultiThreadScan<? extends FilteringScanner> scan = null;
        switch (args[6]) {
            case "q03":
                scan = new MultiThreadFilterScan<>(Q03_Worker.class, args[0] + "q03_col.avsc", args[1],
                        args[2] + "q03_col.json", Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                        Integer.parseInt(args[5]));
                System.out.println("Init eclipse: " + (System.currentTimeMillis() - begin));
                break;
            case "q10":
                scan = new MultiThreadFilterScan<>(Q10_Worker.class, args[0] + "q10_col.avsc", args[1],
                        args[2] + "q10_col.json", Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                        Integer.parseInt(args[5]));
                System.out.println("Init eclipse: " + (System.currentTimeMillis() - begin));
                break;
            case "q15":
                scan = new MultiThreadFilterScan<>(Q15_Worker.class, args[0] + "q15_col.avsc", args[1],
                        args[2] + "q15_col.json", Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                        Integer.parseInt(args[5]));
                System.out.println("Init eclipse: " + (System.currentTimeMillis() - begin));
                break;
        }
        scan.scan();
        System.out.println("Scan eclipse: " + (System.currentTimeMillis() - begin));
    }
}

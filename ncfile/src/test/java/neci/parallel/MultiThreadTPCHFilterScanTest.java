/**
 * 
 */
package neci.parallel;

import java.io.IOException;

import neci.worker.tpch.Q15_Worker;

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
        if (args.length != 6) {
            System.out.println(
                    "Command: String schemaPath, String targetPath, String jsonquery, int degree, int batchSize, int blockSize");
            System.exit(0);
        }
        long begin = System.currentTimeMillis();
        MultiThreadScan<Q15_Worker> scan = new MultiThreadFilterScan<>(Q15_Worker.class, args[0], args[1], args[2],
                Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]));
        System.out.println("Init eclipse: " + (System.currentTimeMillis() - begin));
        scan.scan();
        System.out.println("Scan eclipse: " + (System.currentTimeMillis() - begin));
    }
}

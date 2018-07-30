/**
 * 
 */
package neci.parallel;

import java.io.IOException;

import neci.parallel.worker.FilterScanThread;

/**
 * @author Michael
 *
 */
public class MultiThreadFilterScanTest {

    /**
     * @param args
     * @throws InterruptedException
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args)
            throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        if (args.length != 5) {
            System.out
                    .println("Command: String schemaPath, String targetPath, int degree, int batchSize, int blockSize");
            System.exit(0);
        }
        long begin = System.currentTimeMillis();
        MultiThreadScan<FilterScanThread> scan = new MultiThreadScan<>(FilterScanThread.class, args[0], args[1],
                Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        System.out.println("Init eclipse: " + (System.currentTimeMillis() - begin));
        scan.scan();
        System.out.println("Scan eclipse: " + (System.currentTimeMillis() - begin));
    }

}

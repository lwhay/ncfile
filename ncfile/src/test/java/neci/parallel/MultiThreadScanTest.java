/**
 * 
 */
package neci.parallel;

import java.io.IOException;

import neci.parallel.worker.ScanThread;

/**
 * @author Michael
 *
 */
public class MultiThreadScanTest {

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
        MultiThreadScan<ScanThread> scan = new MultiThreadScan<>(ScanThread.class, args[0], args[1],
                Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        scan.scan();
    }

}

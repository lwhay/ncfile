/**
 * 
 */
package neci.parallel;

import java.io.IOException;

/**
 * @author Michael
 *
 */
public class MultiThreadScanTest {

    /**
     * @param args
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 4) {
            System.out.println("Command: String schemaPath, String targetPath, int degree, int bs");
            System.exit(0);
        }
        MultiThreadScan scan =
                new MultiThreadScan(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        scan.scan();
    }

}

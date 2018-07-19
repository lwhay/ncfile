/**
 * 
 */
package neci.parallel;

import java.io.IOException;

/**
 * @author Michael
 *
 */
public class MultiThreadLoadTest {
    /**
     * @param args
     * @throws InterruptedException
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 8) {
            System.out.println(
                    "Command: String sPath, String dPath, String tPath, int wc, int mul, int dg, int gran, String codec");
            System.exit(0);
        }
        MultiThreadLoad builder =
                new MultiThreadLoad(args[0], args[1], args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                        Integer.parseInt(args[5]), Integer.parseInt(args[6]), args[7]);
        builder.build();
    }

}

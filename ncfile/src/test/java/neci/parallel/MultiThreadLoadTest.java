/**
 * 
 */
package neci.parallel;

import java.io.IOException;

import neci.parallel.worker.BuildThread;

/**
 * @author Michael
 *
 */
public class MultiThreadLoadTest {
    /**
     * @param args
     * @throws InterruptedException
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args)
            throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        if (args.length != 9) {
            System.out.println(
                    "Command: String sPath, String dPath, String tPath, int wc, int mul, int dg, int gran, String codec, int bs");
            System.exit(0);
        }
        MultiThreadLoad<BuildThread> builder = new MultiThreadLoad<>(BuildThread.class, args[0], args[1], args[2],
                Integer.parseInt(args[8]), Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                Integer.parseInt(args[5]), Integer.parseInt(args[6]), args[7]);
        builder.build();
    }

}

/**
 * 
 */
package neci.parallel;

import java.io.IOException;

import neci.parallel.worker.GroupBuilder;

/**
 * @author Michael
 *
 */
public class MultiThreadGroupLoadTest {

    /**
     * @param args
     */
    public static void main(String[] args)
            throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        if (args.length != 9) {
            System.out.println(
                    "Command: String sPath, String dPath, String tPath, int wc, int mul, int dg, int gran, String codec, int blockSize");
            System.exit(0);
        }
        MultiThreadLoad<GroupBuilder> builder = new MultiThreadLoad<>(GroupBuilder.class, args[0], args[1], args[2],
                Integer.parseInt(args[8]), Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                Integer.parseInt(args[5]), Integer.parseInt(args[6]), args[7]);
        long begin = System.currentTimeMillis();
        builder.build();
        builder.verify();
        System.out.println("Build elipse: " + (System.currentTimeMillis() - begin));
    }

}

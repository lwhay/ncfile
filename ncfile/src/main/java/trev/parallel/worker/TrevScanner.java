/**
 * 
 */
package trev.parallel.worker;

import org.apache.avro.Schema;

/**
 * @author Michael
 *
 */
public abstract class TrevScanner implements Runnable {

    public abstract void init(Schema schema, String path, int batchSize);

    public abstract void run();
}

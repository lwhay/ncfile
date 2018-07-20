/**
 * 
 */
package trev.parallel;

import java.io.IOException;

import org.apache.avro.Schema;

import trev.parallel.worker.DremelsScanner;

/**
 * @author Michael
 *
 */
public class DremelsScanThreadFactory<T extends DremelsScanner> {
    final Class<T> scannerClass;
    final Schema schema;
    final String path;
    final int batchSize;

    public DremelsScanThreadFactory(final Class<T> scannerClass, Schema schema, String path, int batchSize)
            throws IOException {
        this.scannerClass = scannerClass;
        this.schema = schema;
        this.path = path;
        this.batchSize = batchSize;
    }

    public T create() throws InstantiationException, IllegalAccessException {
        T builder = scannerClass.newInstance();
        builder.init(schema, path, batchSize);
        return builder;
    }
}

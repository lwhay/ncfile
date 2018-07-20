/**
 * 
 */
package neci.parallel;

import java.io.IOException;

import neci.ncfile.base.Schema;
import neci.parallel.worker.Scanner;

/**
 * @author Michael
 *
 */
public class ScanThreadFactory<T extends Scanner> {
    final Class<T> scannerClass;
    final Schema schema;
    final String path;
    final int batchSize;

    public ScanThreadFactory(final Class<T> scannerClass, Schema schema, String path, int batchSize)
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

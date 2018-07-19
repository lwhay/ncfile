/**
 * 
 */
package neci.parallel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

/**
 * @author Michael
 *
 */
public class MultiThreadScan {
    private final Schema schema;

    private final String targetPath;

    private final int degree;

    private final int batchSize;

    private final Thread[] threads;

    //private static List<List<String>> payloads = new ArrayList<List<String>>();

    private List<FilterBatchColumnReader<Record>> readers = new ArrayList<>();

    public MultiThreadScan(String schemaPath, String targetPath, int degree, int bs) throws IOException {
        this.schema = (new Schema.Parser()).parse(new File(schemaPath));
        this.targetPath = targetPath;
        this.degree = degree;
        this.batchSize = bs;
        this.threads = new Thread[degree];
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public void scan() throws IOException, InterruptedException {
        for (int i = 0; i < degree; i++) {
            String path = targetPath + i + "/result.neci";
            if (!new File(path).exists()) {
                continue;
            }
            readers.add(new FilterBatchColumnReader<>(new File(path)));
            readers.get(i).createSchema(schema);
            readers.get(i).createRead(batchSize);
            Runnable worker = new ScanThread(readers.get(i), schema, path);
            threads[i] = new Thread(worker);
            threads[i].start();
            threads[i].join();
        }
    }
}

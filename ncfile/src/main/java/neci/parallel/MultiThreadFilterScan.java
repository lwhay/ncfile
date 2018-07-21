/**
 * 
 */
package neci.parallel;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import neci.parallel.worker.FilteringScanner;

/**
 * @author Michael
 *
 */
public class MultiThreadFilterScan<T extends FilteringScanner> extends MultiThreadScan<T> {
    protected final String queryPath;

    public MultiThreadFilterScan(Class<T> scannerClass, String schemaPath, String targetPath, String queryPath,
            int degree, int bs) throws IOException {
        super(scannerClass, schemaPath, targetPath, degree, bs);
        this.queryPath = queryPath;
    }

    public void scan() throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        for (int i = 0; i < degree; i++) {
            String path = targetPath + i + "/result.neci";
            if (!new File(path).exists()) {
                continue;
            }
            workers.add(new ScanThreadFactory<T>(scannerClass, schema, path, batchSize * DEFAULT_READ_SCALE).create());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode queryCond = mapper.readTree(new File(queryPath));
            workers.get(i).config(queryCond);
            threads[i] = new Thread(workers.get(i));
            threads[i].start();
        }
        for (int i = 0; i < degree; i++) {
            threads[i].join();
        }
    }
}

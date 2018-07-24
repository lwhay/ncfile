/**
 * 
 */
package trev.parallel;

import java.io.File;
import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import trev.parallel.worker.DremelsFilterScanner;

/**
 * @author Michael
 *
 */
public class DremelsMultiThreadFilterScan<T extends DremelsFilterScanner> extends DremelsMultiThreadScan<T> {
    private final String queryPath;

    public DremelsMultiThreadFilterScan(Class<T> scannerClass, String schemaPath, String targetPath, String queryPath,
            int degree, int bs, String type) throws IOException {
        super(scannerClass, schemaPath, targetPath, degree, bs, type);
        this.queryPath = queryPath;
        // TODO Auto-generated constructor stub
    }

    public void scan() throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        for (int i = 0; i < degree; i++) {
            String path = targetPath + i + "/result." + type;
            if (!new File(path).exists()) {
                continue;
            }
            workers.add(new DremelsScanThreadFactory<T>(scannerClass, schema, path, batchSize * DEFAULT_READ_SCALE)
                    .create());
            threads[i] = new Thread(workers.get(i));
            ObjectMapper mapper = new ObjectMapper();
            JsonNode queryCond = mapper.readTree(new File(queryPath));
            workers.get(i).config(queryCond);
            threads[i].start();
        }
        for (int i = 0; i < degree; i++) {
            threads[i].join();
        }
    }
}

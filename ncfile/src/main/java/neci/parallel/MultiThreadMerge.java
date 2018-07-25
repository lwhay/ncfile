/**
 * 
 */
package neci.parallel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData;
import neci.ncfile.generic.GenericData.Record;
import neci.parallel.worker.BuildThread;
import neci.parallel.worker.MergeThread;

/**
 * @author Michael
 *
 */
public class MultiThreadMerge<T extends MergeThread> {

    private final Class<T> buildClass;

    private final String dataPath;

    private final String targetPath;

    private final int blockSize;

    private final int degree;

    @SuppressWarnings("unused")
    private final int gran;

    private final int wc; // Write count

    private final int mul;

    private final Thread[] threads;

    private final Schema schema;

    private final String codec;

    private List<File> loadQueues = new ArrayList<>();

    private List<BatchAvroColumnWriter<GenericData.Record>> writers = new ArrayList<>();

    public MultiThreadMerge(final Class<T> buildClass, String sPath, String dPath, String tPath, int bs, int wc,
            int mul, int dg, int gran, String codec) throws IOException {
        this.buildClass = buildClass;
        this.schema = new Schema.Parser().parse(new File(sPath));
        this.dataPath = dPath;
        this.targetPath = tPath;
        this.blockSize = bs;
        this.degree = dg;
        this.gran = gran;
        this.wc = wc;
        this.mul = mul;
        this.threads = new Thread[degree];
        this.codec = codec;
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void build() throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        for (int i = 0; i < degree; i++) {
            File[] oldfiles = new File(targetPath + i + "/").listFiles();

            if (oldfiles != null) {
                System.out.println("Delete on " + targetPath + i + "/");
                for (File file : oldfiles) {
                    file.delete();
                }
            }
            writers.add(new BatchAvroColumnWriter<>(schema, targetPath + i + "/", wc, mul, blockSize, codec));
        }

        File parentDir = new File(dataPath);
        File[] files = parentDir.listFiles();
        int fid = 0;
        for (File file : files) {
            if (!file.getAbsolutePath().endsWith("neci"))
                continue;

            if (fid > 0 && fid % degree == 0) {
                for (int i = 0; i < degree; i++) {
                    threads[i].join();
                }
                loadQueues.clear();
            }

            int pid = fid % degree;
            loadQueues.add(file);
            Runnable worker = new MergeThreadFactory<T>(buildClass, writers.get(pid), schema, null).create();
            ((MergeThread) (worker)).set(file);
            threads[pid] = new Thread(worker);
            threads[pid].start();
            fid++;
        }
        for (int i = 0; i < degree; i++) {
            if (threads[i] != null) {
                threads[i].join();
            }
        }
        loadQueues.clear();
        for (int i = 0; i < degree; i++) {
            writers.get(i).flush();
            Runnable worker =
                    new BuildThreadFactory<T>(buildClass, writers.get(i), schema, targetPath + i + "/").create();
            ((BuildThread) (worker)).set(null);
            threads[i] = new Thread(worker);
            threads[i].start();
        }
        for (int i = 0; i < degree; i++) {
            threads[i].join();
        }
    }

    public void verify() throws IOException {
        for (int i = 0; i < degree; i++) {
            File[] files = new File(targetPath + i).listFiles();
            for (File file : files) {
                if (file.getAbsolutePath().endsWith("neci")) {
                    FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<>(file.getAbsoluteFile());
                    reader.createSchema(schema);
                    reader.createRead(10);
                    int localcount = 0;
                    while (reader.hasNext()) {
                        @SuppressWarnings("unused")
                        Record record = reader.next();
                        localcount++;
                    }
                    System.out.println(localcount + " at: " + file.getAbsolutePath());
                    reader.close();
                }
            }
        }
    }

}

/**
 * 
 */
package neci.parallel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData;
import neci.ncfile.generic.GenericData.Record;

/**
 * @author Michael
 *
 */
public class MultiThreadLoad {

    private final String dataPath;

    private final String targetPath;

    private final int degree;

    private final int gran;

    private final int wc; // Write count

    private final int mul;

    private final Thread[] threads;

    private final Schema schema;

    private final String codec;

    private List<List<String>> payloads = new ArrayList<List<String>>();

    private List<BatchAvroColumnWriter<GenericData.Record>> writers = new ArrayList<>();

    public MultiThreadLoad(String sPath, String dPath, String tPath, int wc, int mul, int dg, int gran, String codec)
            throws IOException {
        this.schema = new Schema.Parser().parse(new File(sPath));
        this.dataPath = dPath;
        this.targetPath = tPath;
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
     */
    public void build() throws IOException, InterruptedException {
        for (int i = 0; i < degree; i++) {
            payloads.add(new ArrayList<String>());

            File[] oldfiles = new File(targetPath + i + "/").listFiles();

            if (oldfiles != null) {
                System.out.println("Delete on " + targetPath + i + "/");
                for (File file : oldfiles) {
                    file.delete();
                }
            }
            writers.add(new BatchAvroColumnWriter<>(schema, targetPath + i + "/", wc, mul, codec));
        }

        BufferedReader br = new BufferedReader(new FileReader(dataPath));
        String line;
        int count = 0;
        while ((line = br.readLine()) != null) {
            String[] keys = line.split("\\|");
            int pid = Math.abs(keys[0].hashCode()) % degree;
            payloads.get(pid).add(line);
            if (++count % gran == 0) {
                for (int i = 0; i < degree; i++) {
                    Runnable worker = new BuildThread(writers.get(i), schema, null);
                    ((BuildThread) (worker)).set(payloads.get(i));
                    threads[i] = new Thread(worker);
                    threads[i].start();
                }
                for (int i = 0; i < degree; i++) {
                    threads[i].join();
                    payloads.get(i).clear();
                }
            }
        }

        for (int i = 0; i < degree; i++) {
            Runnable worker = new BuildThread(writers.get(i), schema, null);
            ((BuildThread) (worker)).set(payloads.get(i));
            threads[i] = new Thread(worker);
            threads[i].start();
        }
        for (int i = 0; i < degree; i++) {
            threads[i].join();
            payloads.get(i).clear();
        }
        for (int i = 0; i < degree; i++) {
            writers.get(i).flush();
            Runnable worker = new BuildThread(writers.get(i), schema, targetPath + i + "/");
            ((BuildThread) (worker)).set(null);
            threads[i] = new Thread(worker);
            threads[i].start();
        }
        for (int i = 0; i < degree; i++) {
            threads[i].join();
        }
        br.close();
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
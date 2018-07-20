/**
 * 
 */
package neci.parallel.worker;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

/**
 * @author Michael
 *
 */
public class BuildThread extends Builder {
    protected List<String> payload;

    protected Schema schema;

    protected String path;

    protected BatchAvroColumnWriter<Record> writer;

    public void init(BatchAvroColumnWriter<Record> writer, Schema schema, String path) {
        this.writer = writer;
        this.schema = schema;
        this.path = path;
    }

    public void set(List<String> payload) {
        this.payload = payload;
    }

    public void close() throws IOException {
        writer.flush();
    }

    public void merge() {
        File[] files = new File(path).listFiles();
        File[] toBeMerged = new File[files.length / 2];
        int fidx = 0;
        for (File file : files) {
            if (file.getAbsolutePath().endsWith("neci")) {
                System.out.println(this.path + file.getAbsolutePath());
                toBeMerged[fidx++] = file;
            }
        }
        try {
            writer.mergeFiles(toBeMerged);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            writer.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void build() {
        System.out.println(path + " on: " + payload.size());
        for (String line : payload) {
            Record record = new Record(schema);
            String[] fields = line.split("\\|");
            for (int i = 0; i < schema.getFields().size(); i++) {
                switch (schema.getFields().get(i).schema().getType()) {
                    case INT:
                        record.put(i, Integer.parseInt(fields[i]));
                        break;
                    case LONG:
                        record.put(i, Long.parseLong(fields[i]));
                        break;
                    case FLOAT:
                        record.put(i, Float.parseFloat(fields[i]));
                        break;
                    case DOUBLE:
                        record.put(i, Double.parseDouble(fields[i]));
                        break;
                    case BYTES:
                        record.put(i, ByteBuffer.wrap(fields[i].getBytes()));
                        break;
                    case UNION:
                        if (fields[i] != null) {
                            record.put(i, Integer.parseInt(fields[i]));
                        }
                        break;
                    default:
                        record.put(i, fields[i]);
                }
            }
            //System.out.println(record.toString());
            try {
                writer.append(record);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        /*try {
            writer.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        if (payload != null) {
            build();
        } else {
            merge();
        }
    }
}

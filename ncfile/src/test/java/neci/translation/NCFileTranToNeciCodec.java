/**
 * 
 */
package neci.translation;

import java.io.File;
import java.io.IOException;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

/**
 * @author Michael
 *
 */
public class NCFileTranToNeciCodec {
    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Schema readSchema = new Schema.Parser().parse(new File(args[0]));
        Schema writeSchema = new Schema.Parser().parse(new File(args[1]));
        File fromFile = new File(args[2]);
        String toPath = args[3];
        int free = Integer.parseInt(args[4]);
        int mul = Integer.parseInt(args[5]);
        int blockSize = Integer.parseInt(args[6]);
        String codec = args[7];
        long start = System.currentTimeMillis();
        FilterBatchColumnReader<Record> reader = new FilterBatchColumnReader<Record>(fromFile, blockSize);
        reader.createSchema(readSchema);
        reader.createRead(mul);

        BatchAvroColumnWriter<Record> writer =
                new BatchAvroColumnWriter<Record>(writeSchema, toPath, free, mul, blockSize, codec);
        while (reader.hasNext()) {
            Record record = reader.next();
            writer.flush(record);
        }
        writer.flush();
        reader.close();
        File[] files = new File(toPath).listFiles();
        File[] mergeFiles = new File[files.length / 2];
        int fCount = 0;
        for (File neciFile : files) {
            if (neciFile.getAbsolutePath().endsWith("neci")) {
                mergeFiles[fCount++] = neciFile;
            }
        }
        writer.mergeFiles(mergeFiles);
        writer.flush();
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - start));

    }

}

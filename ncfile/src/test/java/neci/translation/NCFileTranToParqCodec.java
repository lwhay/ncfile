/**
 * 
 */
package neci.translation;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.NestManager;

/**
 * @author Michael
 *
 */
public class NCFileTranToParqCodec {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException {
        System.out.println("Command: schemaPath sourcePath targetPath null readBatch codec blockSize pageSize");
        Schema s = new Schema.Parser().parse(new File(args[0]));
        File fromFile = new File(args[1]);
        File toFile = new File(args[2]);
        int max = Integer.parseInt(args[4]);
        if (!toFile.getParentFile().exists()) {
            toFile.getParentFile().mkdirs();
        }
        /*try {
            Logger.getLogger(Class.forName("*")).setLevel(Level.OFF);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
        @SuppressWarnings("unchecked")
        List<Logger> loggers = Collections.<Logger> list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for (Logger logger : loggers) {
            logger.setLevel(Level.OFF);
        }
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(new NullAppender());
        NestManager.shDelete(args[2]);
        CompressionCodecName codec = null;
        switch (args[5]) {
            case "gzip":
                codec = CompressionCodecName.GZIP;
                break;
            case "snappy":
                codec = CompressionCodecName.SNAPPY;
                break;
            case "null":
                codec = CompressionCodecName.UNCOMPRESSED;
                break;
            case "lzo":
                codec = CompressionCodecName.LZO;
            default:
                System.err.println("Only bzip, snappy, null, lzo can be supported");
                System.exit(-1);;
        }
        File file = new File(args[2]);
        if (file.exists()) {
            file.delete();
        }
        AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(new Path(args[2]), s, codec,
                Integer.parseInt(args[6]), Integer.parseInt(args[7]));
        /*System.setOut(new PrintStream("stdout.log"));
        System.setErr(new PrintStream("stderr.log"));*/
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(new NullAppender());

        FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                new FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record>(fromFile);
        reader.createSchema(new neci.ncfile.base.Schema.Parser().parse(new File(args[0])));
        reader.createRead(max);
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender(new NullAppender());
        while (reader.hasNext()) {
            neci.ncfile.generic.GenericData.Record record = reader.next();
            Record r = NCFileTranToTrevCodec.translate(record, s);

            writer.write(r);
        }
        reader.close();
        writer.close();
    }
}

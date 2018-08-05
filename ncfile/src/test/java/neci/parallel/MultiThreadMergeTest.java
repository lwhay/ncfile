/**
 * 
 */
package neci.parallel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.BatchColumnReader;
import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.NestManager;
import neci.parallel.worker.MergeThread;
import neci.translation.NCFileTranToTrevCodec;

/**
 * @author Michael
 *
 */
public class MultiThreadMergeTest {
    private static final boolean NESTED = true;

    public static void create(String[] args) throws IOException {
        NestManager.shDelete(args[1]);
        NestManager.shDelete(args[2]);
        neci.ncfile.base.Schema schema = new neci.ncfile.base.Schema.Parser().parse(new File(args[0]));
        BatchAvroColumnWriter<neci.ncfile.generic.GenericData.Record> writer =
                new BatchAvroColumnWriter<>(schema, args[1], 2000, 2000, 1, "snappy");
        for (int i = 0; i < 100000; i++) {
            neci.ncfile.generic.GenericData.Record r1 = new neci.ncfile.generic.GenericData.Record(schema);
            r1.put(0, i);
            if (NESTED) {
                List<neci.ncfile.generic.GenericData.Record> f1 = new ArrayList<>();
                for (int j = 0; j < 5; j++) {
                    neci.ncfile.generic.GenericData.Record r11 = new neci.ncfile.generic.GenericData.Record(
                            schema.getFields().get(1).schema().getElementType());
                    r11.put(0, i);
                    List<neci.ncfile.generic.GenericData.Record> f2 = new ArrayList<>();
                    for (int k = 0; k < 10; k++) {
                        neci.ncfile.generic.GenericData.Record r12 =
                                new neci.ncfile.generic.GenericData.Record(schema.getFields().get(1).schema()
                                        .getElementType().getFields().get(1).schema().getElementType());
                        r12.put(0, i);
                        f2.add(r12);
                    }
                    r11.put(1, f2);
                    f1.add(r11);
                }
                r1.put(1, f1);
            }
            //System.out.println(r1);
            writer.flush(r1);
        }
        writer.flush();
    }

    public static void scan(String[] args) throws IOException {
        for (int i = 0; i < Integer.parseInt(args[5]); i++) {
            System.out.println("Openning file: " + args[2] + i + "/result.neci");
            neci.ncfile.base.Schema schema = new neci.ncfile.base.Schema.Parser().parse(new File(args[0]));
            BatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                    new BatchColumnReader<>(new File(args[2] + i + "/result.neci"), 1);
            reader.createSchema(schema);
            reader.create();
            int count = 0;
            while (reader.hasNext()) {
                neci.ncfile.generic.GenericData.Record record = reader.next();
                System.out.println(count++ + "@" + record);
            }
            reader.close();
        }
    }

    public static void clear(String[] args) throws IOException {
        NestManager.shDelete(args[1]);
        NestManager.shDelete(args[2]);
    }

    public static void trev(String[] args) throws IOException {
        Schema s = new Schema.Parser().parse(new File(args[0]));
        File fromFile = new File(args[2] + "0/result.neci");
        File toFile1 = new File(args[2] + "0/result.trev");
        int max = 1000;
        ColumnFileMetaData metaData = new ColumnFileMetaData().setCodec(args[7]);
        AvroColumnWriter<Record> writer1 = new AvroColumnWriter<Record>(s, metaData);
        if (!toFile1.getParentFile().exists()) {
            toFile1.getParentFile().mkdirs();
        }

        FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record> reader =
                new FilterBatchColumnReader<neci.ncfile.generic.GenericData.Record>(fromFile);
        reader.createSchema(new neci.ncfile.base.Schema.Parser().parse(new File(args[0])));
        //        long t1 = System.currentTimeMillis();
        //        reader.filterNoCasc();
        //        long t2 = System.currentTimeMillis();
        reader.createRead(max);

        while (reader.hasNext()) {
            neci.ncfile.generic.GenericData.Record record = reader.next();
            Record r = NCFileTranToTrevCodec.translate(record, s);
            //System.out.println(r);
            //writer1.write(r);
            writer1.write(r);
        }
        reader.close();
        writer1.writeTo(toFile1);
    }

    /**
     * @param args
     * @throws InterruptedException
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args)
            throws IOException, InterruptedException, InstantiationException, IllegalAccessException {
        create(args);
        if (args.length != 9) {
            System.out.println(
                    "Command: String sPath, String dPath, String tPath, int wc, int mul, int dg, int gran, String codec, int blockSize");
            System.exit(0);
        }
        long begin = System.currentTimeMillis();
        MultiThreadMerge<MergeThread> builder = new MultiThreadMerge<>(MergeThread.class, args[0], args[1], args[2],
                Integer.parseInt(args[8]), Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                Integer.parseInt(args[5]), Integer.parseInt(args[6]), args[7]);
        builder.build();
        System.out.println("Merge elipse: " + (System.currentTimeMillis() - begin));
        scan(args);
        trev(args);
        //NestManager.shDelete(args[1]);
        //clear(args);
    }

}

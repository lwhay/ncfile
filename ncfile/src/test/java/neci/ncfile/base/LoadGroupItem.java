package neci.ncfile.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.BatchColumnReader;
import neci.ncfile.FilterBatchColumnReader;
import neci.ncfile.generic.GenericData.Group;
import neci.ncfile.generic.GenericData.Record;

/**
 * Created by michael on 2018/7/16.
 * Default parameters:
 * ./src/resources/group/groupitem.avsc
 * ./src/resources/group/storage/
 * 1000
 * 1000
 * ./src/resources/tpch/lineitem.tbl
 * ./src/resources/group/group.avsc
 * ./src/resources/group/groupread.avsc
 * build
 * 10
 */
public class LoadGroupItem {
    public static void build(String[] args) throws IOException {
        Schema schema = (new Schema.Parser()).parse(new File(args[0]));
        File[] oldfiles = new File(args[1]).listFiles();
        if (oldfiles != null) {
            System.out.println("Delete on " + args[1]);
            for (File file : oldfiles) {
                file.delete();
            }
        }
        int level = Integer.parseInt(args[2]);
        int mul = Integer.parseInt(args[3]);
        int blockSize = Integer.parseInt(args[9]);
        BatchAvroColumnWriter<Record> writer =
                new BatchAvroColumnWriter<>(schema, args[1], level, mul, blockSize, "snappy");
        BufferedReader br = new BufferedReader(new FileReader(args[4]));
        String line;
        Schema gs = new Schema.Parser().parse(new File(args[5]));
        while ((line = br.readLine()) != null) {
            Record record = new Record(schema);
            String[] fields = line.split("\\|");
            for (int i = 0; i < 3; i++) {
                record.put(i, Long.parseLong(fields[i]));
            }
            record.put(3, Integer.parseInt(fields[3]));
            if (fields[7] != null)
                record.put(4, Float.parseFloat(fields[7]));
            Group group = new Group(gs);
            group.put(0, Float.parseFloat(fields[4]));
            group.put(1, Float.parseFloat(fields[5]));
            group.put(2, Float.parseFloat(fields[6]));
            group.put(3, fields[10]);
            record.put(5, group);
            record.put(6, ByteBuffer.wrap(fields[8].getBytes()));
            record.put(7, ByteBuffer.wrap(fields[9].getBytes()));
            record.put(8, fields[11]);
            record.put(9, fields[12]);
            record.put(10, fields[13]);
            record.put(11, fields[14]);
            record.put(12, fields[15]);
            writer.append(record);
        }
        writer.flush();
        br.close();
        File[] files = new File(args[1]).listFiles();
        File[] toBeMerged = new File[files.length / 2];
        int fidx = 0;
        for (File file : files) {
            if (file.getAbsolutePath().endsWith("neci")) {
                toBeMerged[fidx++] = file;
            }
        }
        writer.mergeFiles(toBeMerged);
        writer.flush();
        br.close();
    }

    public static void scan(String[] args) throws IOException {
        Schema schema = (new Schema.Parser()).parse(new File(args[6]));
        BatchColumnReader<Record> fr =
                new BatchColumnReader<>(new File(args[1] + "/result.neci"), Integer.parseInt(args[9]));
        fr.createSchema(schema);
        fr.create();
        String name = schema.getFields().get(0).name();
        int columnNo = fr.getValidColumnNO(name);
        System.out.println("column: " + fr.getTypes().length + " row: " + fr.getRowCount(columnNo));
        while (fr.hasNext()) {
            Record record = fr.next();
            /*System.out.println(record.getSchema().getFields().size());
            System.out.println(record.toString());*/
        }
        fr.close();
    }

    public static void filterScan(String[] args) throws IOException {
        Schema schema = (new Schema.Parser()).parse(new File(args[6]));
        FilterBatchColumnReader<Record> fr =
                new FilterBatchColumnReader<>(new File(args[1] + "/result.neci"), Integer.parseInt(args[9]));
        fr.createSchema(schema);
        fr.createRead(Integer.parseInt(args[8]));
        System.out.println("column: " + fr.getTypes().length + " row: "
                + fr.getRowCount(fr.getValidColumnNO(schema.getFields().get(0).name())));
        while (fr.hasNext()) {
            Record record = fr.next();
            /*System.out.println(record.getSchema().getFields().size());
            System.out.println(record.toString());*/
        }
        fr.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 10) {
            System.out.println(
                    "Command: dataSchema stroage loadGran multation source inlienSchema querySecheam type batchSize blockSize");
            System.exit(0);
        }
        if (args[7].equals("build")) {
            build(args);
            scan(args);
            filterScan(args);
        } else if (args[7].equals("scan")) {
            long begin = System.currentTimeMillis();
            scan(args);
            System.out.println("batch load: " + (System.currentTimeMillis() - begin));
        } else {
            long begin = System.currentTimeMillis();
            filterScan(args);
            System.out.println("fitlerbatch load: " + (System.currentTimeMillis() - begin));
        }
    }
}

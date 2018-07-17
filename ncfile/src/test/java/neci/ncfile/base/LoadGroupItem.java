package neci.ncfile.base;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.generic.GenericData.Record;
import neci.ncfile.generic.GenericData.Group;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by michael on 2018/7/16.
 */
public class LoadGroupItem {
    public static void build(String[] args) throws IOException {
        Schema schema = (new Schema.Parser()).parse(new File(args[0]));
        int level = Integer.parseInt(args[2]);
        int mul = Integer.parseInt(args[3]);
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<>(schema, args[1], level, mul);
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
            record.put(4, fields[7]);
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
    }

    public static void main(String[] args) throws IOException {
        build(args);
    }
}

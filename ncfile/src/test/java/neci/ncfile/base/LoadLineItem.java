package neci.ncfile.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.generic.GenericData;
import neci.ncfile.generic.GenericData.Record;

/**
 * Created by michael on 2018/7/16.
 * Default parameters:
 * ./src/resources/group/lineitem.avsc ./src/resources/group/storage/ 1000 1000 ./src/resources/tpch/lineitem.tbl
 */
public class LoadLineItem {
    public static void build(String[] args) throws IOException {
        Schema schema = (new Schema.Parser()).parse(new File(args[0]));
        File[] oldfiles = new File(args[1]).listFiles();
        if (oldfiles != null) {
            System.out.println("Delete on " + args[1]);
            for (File file : oldfiles) {
                file.delete();
            }
        }
        int free = Integer.parseInt(args[2]);
        int mul = Integer.parseInt(args[3]);
        BatchAvroColumnWriter<GenericData.Record> writer = new BatchAvroColumnWriter<>(schema, args[1], free, mul);
        BufferedReader br = new BufferedReader(new FileReader(args[4]));
        String line;
        while ((line = br.readLine()) != null) {
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
                    default:
                        record.put(i, fields[i]);
                }
            }
            writer.append(record);
        }
        writer.flush();
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

package neci.nest.ppsl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import neci.ncfile.BatchAvroColumnWriter;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.generic.GenericData.Record;

public class NCFile_Flat_Codec {
    private static int blockSize;

    static int tran(String path, String schema, String resultPath, int free, int mul) throws IOException {
        Schema s = new Schema.Parser().parse(new File(schema));
        List<Field> fs = s.getFields();
        BatchAvroColumnWriter<Record> writer = new BatchAvroColumnWriter<Record>(s, resultPath, free, mul, blockSize);
        BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record data = new Record(s);
            for (int i = 0; i < fs.size(); i++) {
                switch (fs.get(i).schema().getType()) {
                    case INT:
                        data.put(i, Integer.parseInt(tmp[i]));
                        break;
                    case LONG:
                        data.put(i, Long.parseLong(tmp[i]));
                        break;
                    case FLOAT:
                        data.put(i, Float.parseFloat(tmp[i]));
                        break;
                    case DOUBLE:
                        data.put(i, Double.parseDouble(tmp[i]));
                        break;
                    case BYTES:
                        data.put(i, ByteBuffer.wrap(tmp[i].getBytes()));
                        break;
                    default:
                        data.put(i, tmp[i]);
                }
            }
            writer.flush(data);
        }
        reader.close();
        int index = writer.flush();
        return index;
    }

    public static void main(String[] args) throws IOException {
        String path = args[0];
        String schema = args[1];
        String result = args[2];
        int free = Integer.parseInt(args[3]);
        int mul = Integer.parseInt(args[4]);
        String codec = args[5];
        blockSize = Integer.parseInt(args[6]);

        long start = System.currentTimeMillis();
        int index = tran(path + "supplier.tbl", schema, result, free, mul);
        long end = System.currentTimeMillis();
        System.out.println("+++++++flattable tran time+++++++" + (end - start));

        String resultPath = result + "/result/";
        Schema s = new Schema.Parser().parse(new File(schema));
        BatchAvroColumnWriter<Record> writer =
                new BatchAvroColumnWriter<Record>(s, resultPath, free, mul, blockSize, codec);
        File[] files = new File[index];
        for (int i = 0; i < index; i++)
            files[i] = new File(result + "file" + String.valueOf(i) + ".neci");
        writer.mergeFiles(files);
        System.out.println("merge completed!");
    }
}

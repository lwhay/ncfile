package kvstore;

import java.io.File;
import java.io.IOException;

import neci.ncfile.ColumnReader;
import neci.ncfile.generic.GenericData.Record;

public class TmpTest {
    public static void main(String[] args) throws IOException {
        ColumnReader<Record> reader =
                new ColumnReader<Record>(new File("/home/lwh/Research/Cores/necirun/test/result/result.trv"));
        int len = reader.getRowCount(1);
        System.out.println(reader.readValue(1));
        reader.create();
        for (int i = 0; i < len; i++) {
            System.out.println(reader.readValue(1));
        }
    }
}

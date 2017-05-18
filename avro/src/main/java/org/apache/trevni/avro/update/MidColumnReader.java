package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.trevni.update.FileColumnMetaData;
import org.apache.trevni.update.MidColumnValues;

public class MidColumnReader<D> extends ColumnReader<D> {
    private Integer[] keyFieldsNO;

    public MidColumnReader(File file, Schema[] keySchemas) throws IOException {
        super(file);
        create(keySchemas);
    }

    public MidColumnReader(File file, GenericData model, Schema[] keySchemas) throws IOException {
        super(file, model);
        create(keySchemas);
    }

    private void create(Schema[] keySchemas) throws IOException {
        List<Integer> no = new ArrayList<Integer>();
        for (Schema s : keySchemas) {
            AvroColumnator readColumnator = new AvroColumnator(s);
            FileColumnMetaData[] readColumns = readColumnator.getColumns();
            for (int i = 0; i < readColumns.length; i++) {
                no.add(reader.getColumnNumber(readColumns[i].getName()));
            }
        }
        Collections.sort(no);
        keyFieldsNO = new Integer[no.size()];
        no.toArray(keyFieldsNO);

        int le = 0;
        for (int i = 0; i < values.length; i++) {
            if (i < keyFieldsNO[le]) {
                values[i] = (MidColumnValues) values[i];
            } else {
                le++;
            }
        }
    }
}

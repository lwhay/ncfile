package neci.ncfile;

import static neci.ncfile.AvroColumnator.isSimple;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.trevni.TrevniRuntimeException;

import columnar.BlockColumnValues;
import columnar.MidInsertColumnFileReader;
import metadata.FileColumnMetaData;
import misc.ValueType;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.generic.GenericData;

public class MidColumnReader<D> extends ColumnReader<D> {
    private Integer[] keyNO;
    MidInsertColumnFileReader reader;

    public MidColumnReader(File file, int[][] keyFields) throws IOException {
        this(file, GenericData.get(), keyFields);
    }

    public MidColumnReader(File file, GenericData model, int[][] keyFields) throws IOException {
        keyNO = new Integer[keyFields.length];
        for (int i = 0; i < keyFields.length; i++)
            keyNO[i] = keyFields[i].length;
        this.reader = new MidInsertColumnFileReader(file);
        columnsByName = reader.getColumnsByName();
        super.model = model;
        this.values = new BlockColumnValues[reader.getColumnCount()];
        int le = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = reader.getValues(i);
            if (values[i].isArray()) {
                le++;
            }
        }
        int j = 0;
        arrayValues = new int[le];
        for (int i = 0; i < le; i++) {
            while (values[j].getType() != ValueType.NULL)
                j++;
            arrayValues[i] = j;
            j++;
        }
    }

    public void createSchema(Schema s) {
        readSchema = s;
        AvroColumnator readColumnator = new AvroColumnator(s);
        FileColumnMetaData[] readColumns = readColumnator.getColumns();
        arrayWidths = readColumnator.getArrayWidths();
        readNO = new int[readColumns.length];
        for (int i = 0; i < readColumns.length; i++) {
            readNO[i] = reader.getColumnNumber(readColumns[i].getName());
        }
    }

    @Override
    public D search(Schema readSchema, int row) {
        AvroColumnator readColumnator = new AvroColumnator(readSchema);
        FileColumnMetaData[] readColumns = readColumnator.getColumns();
        arrayWidths = readColumnator.getArrayWidths();
        readNO = new int[readColumns.length];
        for (int i = 0; i < readColumns.length; i++) {
            readNO[i] = reader.getColumnNumber(readColumns[i].getName());
        }
        try {
            column = 0;
            return (D) read(readSchema, row);
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    @Override
    public Object read(Schema s, int row) throws IOException {
        if (isSimple(s)) {
            return readValue(s, readNO[column++], row);
        }
        final int startColumn = column;

        switch (s.getType()) {
            case RECORD:
                Object record = model.newRecord(null, s);
                for (Field f : s.getFields()) {
                    Object value = read(f.schema(), row);
                    model.setField(record, f.name(), f.pos(), value);
                }
                return record;
            case ARRAY:
                int length;
                int offset = 0;
                if (row == 0) {
                    values[readNO[column]].seek(0);
                    length = values[readNO[column]].nextLength();
                } else {
                    values[readNO[column]].seek(row - 1);
                    values[readNO[column]].startRow();
                    values[readNO[column]].nextLengthAndOffset();
                    values[readNO[column]].startRow();
                    int[] rr = values[readNO[column]].nextLengthAndOffset();
                    length = rr[0];
                    offset = rr[1];
                }
                List elements = (List) new GenericData.Array(length, s);
                for (int i = 0; i < length; i++) {
                    this.column = startColumn;
                    Object value;
                    if (isSimple(s.getElementType()))
                        value = readValue(s, readNO[++column], (offset + i));
                    else {
                        column++;
                        value = read(s.getElementType(), (offset + i));
                    }
                    elements.add(value);
                }
                column = startColumn + arrayWidths[startColumn];
                return elements;
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
    }

    @Override
    public Object readValue(Schema s, int column, int row) throws IOException {
        values[column].seek(row);
        return readValue(s, column);
    }

    @Override
    public Object readValue(Schema s, int column) throws IOException {
        values[column].startRow();
        Object v = values[column].nextValue();

        switch (s.getType()) {
            case ENUM:
                return model.createEnum(s.getEnumSymbols().get((Integer) v), s);
            case FIXED:
                return model.createFixed(null, ((ByteBuffer) v).array(), s);
        }

        return v;
    }

    //    private void create(Schema[] keySchemas) throws IOException {
    //        List<Integer> no = new ArrayList<Integer>();
    //        for (Schema s : keySchemas) {
    //            AvroColumnator readColumnator = new AvroColumnator(s);
    //            FileColumnMetaData[] readColumns = readColumnator.getColumns();
    //            for (int i = 0; i < readColumns.length; i++) {
    //                no.add(reader.getColumnNumber(readColumns[i].getName()));
    //            }
    //        }
    //        Collections.sort(no);
    //        keyFieldsNO = new Integer[no.size()];
    //        no.toArray(keyFieldsNO);
    //
    //        int le = 0;
    //        for (int i = 0; i < values.length; i++) {
    //            if (i < keyFieldsNO[le]) {
    //                values[i] = (MidColumnValues) values[i];
    //            } else {
    //                le++;
    //            }
    //        }
    //    }
}

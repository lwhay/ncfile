package neci.ncfile;

import static neci.ncfile.AvroColumnator.isSimple;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.trevni.TrevniRuntimeException;

import columnar.BatchColumnFileReader;
import columnar.BlockColumnValues;
import columnar.BlockManager;
import metadata.FileColumnMetaData;
import misc.GroupCore;
import misc.ValueType;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.base.Schema.Type;
import neci.ncfile.generic.GenericData;
import neci.ncfile.generic.GenericGroupReader;

public class BatchColumnReader<D> implements Closeable {
    BatchColumnFileReader reader;
    protected GenericData model;
    protected BlockColumnValues[] values;
    protected boolean[] valids;
    protected int[] readNO;
    protected int[] arrayWidths;
    protected int column;
    /*protected int[] arrayValues;*/
    protected GenericGroupReader groupReader;
    protected HashMap<String, Integer> columnsByName;
    protected Schema readSchema;

    public BatchColumnReader() {

    }

    public BatchColumnReader(File file) throws IOException {
        this(file, GenericData.get(), BatchColumnFileReader.DEFAULT_BLOCK_SIZE);
    }

    public BatchColumnReader(File file, int blockSize) throws IOException {
        this(file, GenericData.get(), blockSize);
    }

    public BatchColumnReader(File file, GenericData model, int bs) throws IOException {
        this.reader = new BatchColumnFileReader(file, bs);
        columnsByName = reader.getColumnsByName();
        this.model = model;
        this.values = new BlockColumnValues[reader.getColumnCount()];
    }

    public BlockManager getBlockManager() {
        return reader.getBlockManager();
    }

    public int getValidColumnNO(String name) {
        if (readSchema.getField(name).schema().getType().equals(Type.ARRAY)) {
            name += "[]";
        }
        Integer ctm = columnsByName.get(name);
        int tm = 0;
        for (; tm < readNO.length; tm++) {
            if (readNO[tm] == ctm) {
                break;
            }
        }
        if (tm >= readNO.length)
            throw new TrevniRuntimeException("No column named: " + name);
        return ctm;
    }

    public int getColumnNO(String name) {
        if ((columnsByName.get(name)) == null)
            throw new TrevniRuntimeException("No column named: " + name);
        return columnsByName.get(name);
    }

    public ValueType getType(int columnNo) {
        return values[columnNo].getType();
    }

    public ValueType[] getTypes() {
        ValueType[] res = new ValueType[values.length];
        for (int i = 0; i < res.length; i++) {
            if (valids[i])
                res[i] = values[i].getType();
        }
        return res;
    }

    public void createSchema(Schema s) throws IOException {
        readSchema = s;
        AvroColumnator readColumnator = new AvroColumnator(s);
        FileColumnMetaData[] readColumns = readColumnator.getColumns();
        arrayWidths = readColumnator.getArrayWidths();
        readNO = new int[readColumns.length];
        Set<String> validColumns = new HashSet<>();
        for (int i = 0; i < readColumns.length; i++) {
            validColumns.add(readColumns[i].getName());
        }
        reader.readColumnInfo(validColumns);
        valids = new boolean[columnsByName.size()];
        for (Entry<String, Integer> pair : columnsByName.entrySet()) {
            if (validColumns.contains(pair.getKey())) {
                values[pair.getValue()] = reader.getValues(pair.getValue());
                valids[pair.getValue()] = true;
            } else {
                valids[pair.getValue()] = false;
            }
        }
        /*int le = 0;
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
        }*/
        for (int i = 0; i < readColumns.length; i++) {
            readNO[i] = reader.getColumnNumber(readColumns[i].getName());
        }
    }

    public D next() {
        try {
            column = 0;
            return (D) read(readSchema);
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    public int[] nextKey() throws IOException {
        int[] res = new int[readNO.length];
        for (int i = 0; i < res.length; i++) {
            values[readNO[i]].startRow();
            res[i] = values[readNO[i]].nextKey();
        }
        return res;
    }

    public boolean hasNext() {
        return values[readNO[0]].hasNext();
    }

    public boolean hasNext(int no) {
        return values[no].hasNext();
    }

    public D[] search(Schema readSchema, int row, int no) {
        AvroColumnator readColumnator = new AvroColumnator(readSchema);
        FileColumnMetaData[] readColumns = readColumnator.getColumns();
        arrayWidths = readColumnator.getArrayWidths();
        readNO = new int[readColumns.length];
        for (int i = 0; i < readColumns.length; i++) {
            readNO[i] = reader.getColumnNumber(readColumns[i].getName());
        }
        try {
            column = 0;
            List<D> res = new ArrayList<D>();
            res.add((D) read(readSchema, row));
            for (int i = 1; i < no; i++) {
                res.add((D) read(readSchema));
            }
            return (D[]) res.toArray();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    public D[] search(int row, int no) {
        try {
            column = 0;
            List<D> res = new ArrayList<D>();
            res.add((D) read(readSchema, row));
            for (int i = 1; i < no; i++) {
                res.add((D) read(readSchema));
            }
            return (D[]) res.toArray();
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

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

    public D search(int row) {
        try {
            column = 0;
            return (D) read(readSchema, row);
        } catch (IOException e) {
            throw new TrevniRuntimeException(e);
        }
    }

    /*public int searchArray(int row, int le) throws IOException {
        values[arrayValues[le]].seek(row);
        return nextArray(le);
    }
    
    public int searchArray(int row, int le, int no) throws IOException {
        values[arrayValues[le]].seek(row);
        int res = 0;
        for (int i = 0; i < no; i++) {
            res += nextArray(le);
        }
        return res;
    }
    
    public int nextArray(int le) throws IOException {
        values[arrayValues[le]].startRow();
        return values[arrayValues[le]].nextLength();
    }*/

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
                    values[readNO[column]].startBlock(0);
                    length = values[readNO[column]].nextLength();
                } else {
                    values[readNO[column]].startBlock(row - 1);
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
            //                int newRow = 0;
            //                values[readNO[column]].startBlock(0);
            //                for (int i = 0; i < row; i++) {
            //                    newRow += values[readNO[column]].nextLength();
            //                    values[readNO[column]].startRow();
            //                }
            //                int length = values[readNO[column]].nextLength();
            //                List elements = (List) new GenericData.Array(length, s);
            //                for (int i = 0; i < length; i++) {
            //                    this.column = startColumn;
            //                    Object value;
            //                    if (isSimple(s.getElementType()))
            //                        value = readValue(s, ++column, (newRow + i));
            //                    else {
            //                        column++;
            //                        value = read(s.getElementType(), (newRow + i));
            //                    }
            //                    elements.add(value);
            //                }
            //                column = startColumn + arrayWidths[startColumn];
            //                return elements;
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
    }

    public Object read(Schema s) throws IOException {
        if (isSimple(s)) {
            return readValue(s, readNO[column++]);
        }
        final int startColumn = column;

        switch (s.getType()) {
            case RECORD:
                Object record = model.newRecord(null, s);
                for (Field f : s.getFields()) {
                    Object value = read(f.schema());
                    model.setField(record, f.name(), f.pos(), value);
                }
                return record;
            case ARRAY:
                int length;

                values[readNO[column]].startRow();
                int[] rr = values[readNO[column]].nextLengthAndOffset();
                length = rr[0];
                //                offset = rr[1];
                //                int length = values[readNO[column]].nextLength();
                List elements = (List) new GenericData.Array(length, s);
                for (int i = 0; i < length; i++) {
                    this.column = startColumn;
                    Object value;
                    if (isSimple(s.getElementType()))
                        value = readValue(s, readNO[++column]);
                    else {
                        column++;
                        value = read(s.getElementType());
                    }
                    elements.add(value);
                }
                column = startColumn + arrayWidths[startColumn];
                return elements;
            default:
                throw new TrevniRuntimeException("Unknown schema: " + s);
        }
    }

    public Object readValue(Schema s, int column, int row) throws IOException {
        values[column].seek(row);
        return readValue(s, column);
    }

    public Object readValue(int column, int row) throws IOException {
        values[column].seek(row);
        return readValue(column);
    }

    public void skipValue(int column, int row) throws IOException {
        values[column].seek(row);
        skipValue(column);
    }

    public Object readValue(Schema s, int column) throws IOException {
        values[column].startRow();
        Object v = values[column].nextValue();

        switch (s.getType()) {
            case GROUP: {
                if (groupReader == null) {
                    groupReader = new GenericGroupReader();
                }
                return groupReader.readGroup((GroupCore) v, s);
            }
            case ENUM:
                return model.createEnum(s.getEnumSymbols().get((Integer) v), s);
            case FIXED:
                return model.createFixed(null, ((ByteBuffer) v).array(), s);
        }

        return v;
    }

    public Object readValue(int column) throws IOException {
        values[column].startRow();
        return values[column].nextValue();
    }

    public void create() throws IOException {
        int i = 0;
        for (BlockColumnValues v : values) {
            if (valids[i++])
                v.create();
        }
    }

    public void create(int no) throws IOException {
        values[no].create();
    }

    /*public int getLevelRowCount(int level) {
        int column;
        if (level == 0)
            column = 0;
        else
            column = arrayValues[level - 1] + 1;
        return getRowCount(column);
    }*/

    public int getRowCount(int columnNo) {
        return values[columnNo].getLastRow();
    }

    public Object nextValue(int columnNo) throws IOException {
        values[columnNo].startRow();
        return values[columnNo].nextValue();
    }

    public void skipValue(int columnNo) throws IOException {
        values[columnNo].startRow();
        values[columnNo].skipValue(1);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

package neci.ncfile;

import static neci.ncfile.AvroColumnator.isSimple;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.trevni.Input;
import org.apache.trevni.InputFile;

import columnar.BatchColumnFileReader;
import columnar.BlockColumnValues;
import columnar.BlockManager;
import exceptions.NeciRuntimeException;
import metadata.FileColumnMetaData;
import misc.GroupCore;
import misc.ValueType;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.base.Schema.Type;
import neci.ncfile.generic.GenericData;
import neci.ncfile.generic.GenericGroupReader;

public class FilterBatchColumnReader<D> implements Closeable {
    BatchColumnFileReader reader;
    protected GenericData model;
    protected GenericGroupReader groupReader;
    protected Set<String> validColumns;
    protected BlockColumnValues[] values;
    protected int[] readNO;
    protected int[] arrayWidths;
    protected int column;
    /*protected int[] arrayValues;*/
    protected HashMap<String, Integer> columnsByName;
    protected Schema readSchema;
    protected FilterOperator[] filters; //ensure that the filters is sorted from small to big layer

    protected String readParent;
    protected HashMap<String, Integer> readLength;
    protected Object[][] readValue;
    protected String currentParent;
    protected int currentLayer;
    protected BitSet filterSet;
    protected ArrayList<BitSet> chooseSet;
    protected HashMap<String, Integer> bitSetMap;
    protected HashMap<String, BitSet> filterSetMap;
    protected int[] readIndex; //the index of the next Record in every column;
    protected int all; //the number of the remain values in the disk;
    protected int[] setStart; //the start of the bitset of every layer
    protected int[] readSet; //the index of chooseSet of every read column

    //    protected long timeIO;
    //    protected int readBlockSize;
    //    protected int seekedBlock;
    //    protected int blockCount;
    //    protected List<Long> blockTime;
    //    protected List<Long> blockStart;
    //    protected List<Long> blockEnd;
    //    protected List<Long> blockOffset;

    protected boolean noFilters;
    protected int currentMax;

    private int defaultMax = 100000;

    public FilterBatchColumnReader() {

    }

    public FilterBatchColumnReader(File file) throws IOException {
        this(file, null, GenericData.get());
    }

    public FilterBatchColumnReader(File file, int bs) throws IOException {
        this(file, null, GenericData.get(), bs);
    }

    public FilterBatchColumnReader(Input data, Input head) throws IOException {
        this(data, head, null, GenericData.get(), BatchColumnFileReader.DEFAULT_BLOCK_SIZE);
    }

    public FilterBatchColumnReader(Input data, Input head, int bs) throws IOException {
        this(data, head, null, GenericData.get(), bs);
    }

    public FilterBatchColumnReader(File file, FilterOperator[] filters) throws IOException {
        this(file, filters, GenericData.get());
    }

    public FilterBatchColumnReader(File file, FilterOperator[] filters, int bs) throws IOException {
        this(file, filters, GenericData.get(), bs);
    }

    public FilterBatchColumnReader(Input data, Input head, FilterOperator[] filters) throws IOException {
        this(data, head, filters, GenericData.get(), BatchColumnFileReader.DEFAULT_BLOCK_SIZE);
    }

    public FilterBatchColumnReader(Input data, Input head, FilterOperator[] filters, int bs) throws IOException {
        this(data, head, filters, GenericData.get(), bs);
    }

    public FilterBatchColumnReader(File file, FilterOperator[] filters, GenericData model) throws IOException {
        this(file, filters, model, BatchColumnFileReader.DEFAULT_BLOCK_SIZE);
    }

    public FilterBatchColumnReader(File file, FilterOperator[] filters, GenericData model, int bs) throws IOException {
        this(new InputFile(file),
                new InputFile(new File(
                        file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + ".head")),
                filters, model, bs);
    }

    public FilterBatchColumnReader(Input data, Input head, FilterOperator[] filters, GenericData model, int bs)
            throws IOException {
        this.reader = new BatchColumnFileReader(data, head, bs);
        this.filters = filters;
        columnsByName = reader.getColumnsByName();
        this.model = model;
        this.values = new BlockColumnValues[reader.getColumnCount()];
        noFilters = (filters == null);
    }

    public BlockManager getBlockManager() {
        return reader.getBlockManager();
    }

    //    public long getTimeIO() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            timeIO += values[readNO[i]].getTime();
    //        }
    //        return timeIO;
    //    }
    //
    //    public List<Long> getBlockTime() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            blockTime.addAll(values[readNO[i]].getBlockTime());
    //        }
    //        return blockTime;
    //    }
    //
    //    public List<Long> getBlockStart() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            blockStart.addAll(values[readNO[i]].getBlockStart());
    //        }
    //        return blockStart;
    //    }
    //
    //    public List<Long> getBlockEnd() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            blockEnd.addAll(values[readNO[i]].getBlockEnd());
    //        }
    //        return blockEnd;
    //    }
    //
    //    public List<Long> getBlockOffset() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            blockOffset.addAll(values[readNO[i]].getBlockOffset());
    //        }
    //        return blockOffset;
    //    }
    //
    //    public int[] getFilterBlock() {
    //        return new int[] { readBlockSize, seekedBlock, blockCount };
    //    }
    //
    //    public int[] getBlockSeekRes() {
    //        for (int i = 0; i < readNO.length; i++) {
    //            readBlockSize += values[readNO[i]].getSeekBlock()[0];
    //            seekedBlock += values[readNO[i]].getSeekBlock()[1];
    //            blockCount += values[readNO[i]].getBlockCount();
    //        }
    //        return new int[] { readBlockSize, seekedBlock, blockCount };
    //    }

    public void filter() throws IOException {
        assert (filters != null);
        //        timeIO = 0;
        //        blockTime = new ArrayList<Long>();
        //        blockStart = new ArrayList<Long>();
        //        blockEnd = new ArrayList<Long>();
        //        blockOffset = new ArrayList<Long>();
        //        readBlockSize = 0;
        //        seekedBlock = 0;
        //        blockCount = 0;
        String column = filters[0].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new NeciRuntimeException("No filter column named: " + column);
        filterSet = new BitSet(values[tm].getLastRow());
        currentParent = values[tm].getParentName();
        currentLayer = values[tm].getLayer();
        int i = 0;
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        BitSet initialSet = new BitSet(values[tm].getLastRow());
        initialSet.set(0, values[tm].getLastRow());
        reader.getBlockManager().trigger(tm, initialSet);
        values[tm].create();
        while (values[tm].hasNext()) {
            if (filters[0].isMatch(values[tm].next())) {
                filterSet.set(i);
            }
            i++;
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
        filterSetMap = new HashMap<String, BitSet>();
        if (currentParent != null)
            filterSetMap.put(currentParent, (BitSet) filterSet.clone());
        for (int c = 1; c < filters.length - 1; c++) {
            filter(c);
            if (currentParent != null)
                filterSetMap.put(currentParent, (BitSet) filterSet.clone());
        }
        if (filters.length > 1)
            filter(filters.length - 1);

        if (currentParent != null) {
            String parent = new String(currentParent);
            while (parent != null) {
                filterSetMap.remove(parent);
                if (filterSetMap.isEmpty()) {
                    break;
                }
                parent = values[columnsByName.get(parent)].getParentName();
            }
        }
        chooseSet = new ArrayList<BitSet>();
        chooseSet.add((BitSet) filterSet.clone());
        bitSetMap = new HashMap<String, Integer>();
        bitSetMap.put(currentParent, 0);
    }

    private void filter(int c) throws IOException {
        String column = filters[c].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new NeciRuntimeException("No filter column named: " + column);
        String parent = values[tm].getParentName();
        int layer = values[tm].getLayer();
        if (layer != currentLayer || (parent != null && !currentParent.equals(parent))) {
            filterSetTran(tm);
        }
        int m = filterSet.nextSetBit(0);
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        reader.getBlockManager().trigger(tm, filterSet);
        values[tm].create();
        while (m != -1) {
            values[tm].seek(m);
            if (!filters[c].isMatch(values[tm].next())) {
                filterSet.set(m, false);
            }
            if (++m > filterSet.length())
                break;
            m = filterSet.nextSetBit(m);
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
    }

    public void filterNoCasc() throws IOException {
        assert (filters != null);
        //        timeIO = 0;
        //        blockTime = new ArrayList<Long>();
        //        blockStart = new ArrayList<Long>();
        //        blockEnd = new ArrayList<Long>();
        //        blockOffset = new ArrayList<Long>();
        //        readBlockSize = 0;
        //        seekedBlock = 0;
        //        blockCount = 0;
        String column = filters[0].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new NeciRuntimeException("No filter column named: " + column);
        filterSet = new BitSet(values[tm].getLastRow());
        currentParent = values[tm].getParentName();
        currentLayer = values[tm].getLayer();
        int i = 0;
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        reader.getBlockManager().setSkip(false);
        reader.getBlockManager().trigger(tm, filterSet);
        values[tm].create();
        while (values[tm].hasNext()) {
            if (filters[0].isMatch(values[tm].next())) {
                filterSet.set(i);
            }
            i++;
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
        filterSetMap = new HashMap<String, BitSet>();
        if (currentParent != null)
            filterSetMap.put(currentParent, (BitSet) filterSet.clone());
        for (int c = 1; c < filters.length - 1; c++) {
            filterNoCasc(c);
            if (currentParent != null)
                filterSetMap.put(currentParent, (BitSet) filterSet.clone());
        }
        if (filters.length > 1)
            filterNoCasc(filters.length - 1);

        if (currentParent != null) {
            String parent = new String(currentParent);
            while (parent != null) {
                filterSetMap.remove(parent);
                if (filterSetMap.isEmpty()) {
                    break;
                }
                parent = values[columnsByName.get(parent)].getParentName();
            }
        }
        chooseSet = new ArrayList<BitSet>();
        chooseSet.add((BitSet) filterSet.clone());
        bitSetMap = new HashMap<String, Integer>();
        bitSetMap.put(currentParent, 0);
    }

    public void filterReadIO() throws IOException {
        assert (filters != null);
        String column = filters[0].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new NeciRuntimeException("No filter column named: " + column);
        currentParent = values[tm].getParentName();
        currentLayer = values[tm].getLayer();
        // Need to be checked.
        reader.getBlockManager().trigger(tm, filterSet);
        values[tm].readIO();
        values[tm].create();
        for (int c = 1; c < filters.length; c++) {
            column = filters[c].getName();
            tm = columnsByName.get(column);
            if (tm == null)
                throw new NeciRuntimeException("No filter column named: " + column);
            String parent = values[tm].getParentName();
            int layer = values[tm].getLayer();
            if (layer != currentLayer || (parent != null && !currentParent.equals(parent))) {
                List<String> left = new ArrayList<String>();
                List<String> right = new ArrayList<String>();
                if (currentLayer > layer) {
                    for (int i = currentLayer; i > layer; i--) {
                        left.add(currentParent);
                        int col = columnsByName.get(currentParent);
                        currentParent = values[col].getParentName();
                    }
                    currentLayer = layer;
                }
                if (layer > currentLayer) {
                    for (int i = layer; i > currentLayer; i--) {
                        right.add(parent);
                        int col = columnsByName.get(parent);
                        parent = values[col].getParentName();
                    }
                    layer = currentLayer;
                }
                while (currentParent != null && !currentParent.equals(parent)) {
                    left.add(currentParent);
                    int l = columnsByName.get(currentParent);
                    currentParent = values[l].getParentName();
                    right.add(parent);
                    int r = columnsByName.get(parent);
                    parent = values[r].getParentName();
                }

                for (int i = 0; i < left.size(); i++) {
                    String array = left.get(i);
                    int col = columnsByName.get(array);
                    // Need to be checked.
                    reader.getBlockManager().trigger(col, filterSet);
                    values[col].readIO();
                    values[col].create();
                }

                for (int i = right.size() - 1; i >= 0; i--) {
                    String array = right.get(i);
                    int col = columnsByName.get(array);
                    // Need to be checked.
                    reader.getBlockManager().trigger(col, filterSet);
                    values[col].readIO();
                    values[col].create();
                }
                currentLayer = values[tm].getLayer();
                currentParent = values[tm].getParentName();
            }
        }
    }

    private void filterNoCasc(int c) throws IOException {
        String column = filters[c].getName();
        Integer tm = columnsByName.get(column);
        if (tm == null)
            throw new NeciRuntimeException("No filter column named: " + column);
        String parent = values[tm].getParentName();
        int layer = values[tm].getLayer();
        if (layer != currentLayer || (parent != null && !currentParent.equals(parent))) {
            filterSetTran(tm);
        }
        BitSet set = new BitSet(values[tm].getLastRow());
        //        values[tm].createTime();
        //        values[tm].createSeekBlock();
        reader.getBlockManager().setSkip(false);
        reader.getBlockManager().trigger(tm, set);
        values[tm].create();
        int n = 0;
        while (values[tm].hasNext()) {
            if (filters[c].isMatch(values[tm].next()))
                set.set(n);
            ++n;
        }
        //        timeIO += values[tm].getTime();
        //        blockTime.addAll(values[tm].getBlockTime());
        //        blockStart.addAll(values[tm].getBlockStart());
        //        blockEnd.addAll(values[tm].getBlockEnd());
        //        blockOffset.addAll(values[tm].getBlockOffset());
        //        readBlockSize += values[tm].getSeekBlock()[0];
        //        seekedBlock += values[tm].getSeekBlock()[1];
        //        blockCount += values[tm].getBlockCount();
        set.and(filterSet);
        filterSet = set;
    }

    private void filterSetTran(int c) throws IOException {
        List<String> left = new ArrayList<String>();
        List<String> right = new ArrayList<String>();
        int layer = values[c].getLayer();
        String parent = values[c].getParentName();
        if (currentLayer > layer) {
            for (int i = currentLayer; i > layer; i--) {
                left.add(currentParent);
                int col = columnsByName.get(currentParent);
                currentParent = values[col].getParentName();
            }
            currentLayer = layer;
        }
        if (layer > currentLayer) {
            for (int i = layer; i > currentLayer; i--) {
                right.add(parent);
                int col = columnsByName.get(parent);
                parent = values[col].getParentName();
            }
            layer = currentLayer;
        }
        while (currentParent != null && !currentParent.equals(parent)) {
            left.add(currentParent);
            int l = columnsByName.get(currentParent);
            currentParent = values[l].getParentName();
            right.add(parent);
            int r = columnsByName.get(parent);
            parent = values[r].getParentName();
        }

        for (int i = 0; i < left.size(); i++) {
            String array = left.get(i);
            upTran(array);
            String arr = values[columnsByName.get(array)].getParentName();
            if (arr != null)
                filterSetMap.put(arr, (BitSet) filterSet.clone());
        }

        for (int i = right.size() - 1; i >= 0; i--) {
            String array = right.get(i);
            downTran(array);
            BitSet f = filterSetMap.get(array);
            if (f != null) {
                filterSet.and(f);
            }
        }
        currentLayer = values[c].getLayer();
        currentParent = values[c].getParentName();
    }

    private void upTran(String array) throws IOException {
        int col = columnsByName.get(array);
        BitSet set = new BitSet(values[col].getLastRow());
        int m = filterSet.nextSetBit(0);
        int n = 0;
        //        values[col].createTime();
        //        values[col].createSeekBlock();
        reader.getBlockManager().trigger(col, filterSet);
        values[col].create();
        while (m != -1 && values[col].hasNext()) {
            values[col].startRow();
            int max = values[col].nextLength();
            if (max > m) {
                set.set(n);
                if (++m > filterSet.length())
                    m = -1;
                else {
                    m = filterSet.nextSetBit(m);
                    while (m != -1 && m < max)
                        m = filterSet.nextSetBit(++m);
                }
            }
            n++;
        }
        /*reader.getBlockManager().invalid(col);*/
        filterSet = set;
        //        timeIO += values[col].getTime();
        //        blockTime.addAll(values[col].getBlockTime());
        //        blockStart.addAll(values[col].getBlockStart());
        //        blockEnd.addAll(values[col].getBlockEnd());
        //        blockOffset.addAll(values[col].getBlockOffset());
        //        readBlockSize += values[col].getSeekBlock()[0];
        //        seekedBlock += values[col].getSeekBlock()[1];
        //        blockCount += values[col].getBlockCount();
    }

    private void downTran(String array) throws IOException {
        int col = columnsByName.get(array);
        int firstChildId = -1;
        for (String childName : validColumns) {
            if (values[columnsByName.get(childName)].getParentName() != null
                    && values[columnsByName.get(childName)].getParentName().equals(array)) {
                firstChildId = columnsByName.get(childName);
            }
        }
        BitSet set = new BitSet(values[firstChildId].getLastRow());
        int p = filterSet.nextSetBit(0);
        int q = -1;
        //        values[col].createTime();
        //        values[col].createSeekBlock();
        reader.getBlockManager().trigger(col, filterSet);
        values[col].create();
        if (p == 0) {
            values[col].startRow();
            int[] res = values[col].nextLengthAndOffset();
            if (res[0] > 0)
                set.set(res[1], res[0] + res[1]);
            q = p;
            p = filterSet.nextSetBit(1);
        }
        while (p != -1) {
            if (p == q + 1) {
                values[col].startRow();
                int[] res = values[col].nextLengthAndOffset();
                if (res[0] > 0)
                    set.set(res[1], res[0] + res[1]);
                //                for (int j = 0; j < res[0]; j++)
                //                    set.set(j + res[1]);
            } else {
                values[col].seek(p - 1);
                values[col].startRow();
                values[col].nextLengthAndOffset();
                values[col].startRow();
                int[] res = values[col].nextLengthAndOffset();
                if (res[0] > 0)
                    set.set(res[1], res[0] + res[1]);
                //                for (int j = 0; j < res[0]; j++)
                //                    set.set(j + res[1]);
            }
            q = p;
            if (++p > filterSet.length())
                break;
            p = filterSet.nextSetBit(p);
        }
        /*reader.getBlockManager().invalid(col);*/
        filterSet = set;
        //        timeIO += values[col].getTime();
        //        blockTime.addAll(values[col].getBlockTime());
        //        blockStart.addAll(values[col].getBlockStart());
        //        blockEnd.addAll(values[col].getBlockEnd());
        //        blockOffset.addAll(values[col].getBlockOffset());
        //        readBlockSize += values[col].getSeekBlock()[0];
        //        seekedBlock += values[col].getSeekBlock()[1];
        //        blockCount += values[col].getBlockCount();
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
            throw new NeciRuntimeException("No column named: " + name);
        return ctm;
    }

    public int getColumnNO(String name) {
        Integer tm = columnsByName.get(name);
        if (tm == null)
            throw new NeciRuntimeException("No column named: " + name);
        return tm;
    }

    public ValueType getType(int columnNo) {
        return values[columnNo].getType();
    }

    public ValueType[] getTypes() {
        ValueType[] res = new ValueType[values.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = values[i].getType();
        }
        return res;
    }

    public BitSet getCurrentSet() {
        return filterSet;
    }

    public ArrayList<BitSet> getReadSet() {
        ArrayList<BitSet> res = new ArrayList<BitSet>();
        for (int x : readSet) {
            res.add(chooseSet.get(x));
        }
        return res;
    }

    public void createSchema(Schema s) throws IOException {
        readSchema = s;
        AvroColumnator readColumnator = new AvroColumnator(s);
        FileColumnMetaData[] readColumns = readColumnator.getColumns();
        arrayWidths = readColumnator.getArrayWidths();
        readNO = new int[readColumns.length];
        //        int le = 0;
        validColumns = new HashSet<>();
        for (int i = 0; i < readColumns.length; i++) {
            validColumns.add(readColumns[i].getName());
            //            if (values[readNO[i]].getType() == ValueType.NULL) {
            //                le++;
            //            }
            //            values[readNO[i]].createLength();
        }

        if (filters != null) {
            for (FilterOperator filter : filters) {
                validColumns.add(filter.getName());
                if (filter instanceof HavingOperator) {
                    validColumns.add(((HavingOperator<?>) filter).getHavingName());
                }
            }
        }
        reader.readColumnInfo(validColumns);
        for (int i = 0; i < readColumns.length; i++) {
            readNO[i] = reader.getColumnNumber(readColumns[i].getName());
            //            if (values[readNO[i]].getType() == ValueType.NULL) {
            //                le++;
            //            }
            //            values[readNO[i]].createLength();
        }
        for (Entry<String, Integer> pair : columnsByName.entrySet()) {
            if (validColumns.contains(pair.getKey()))
                values[pair.getValue()] = reader.getValues(pair.getValue());
        }
        readParent = values[readNO[0]].getParentName();
        //        if (le > 0) {
        //            arrayValues = new int[le];
        //        }
        try {
            reader.getBlockManager().openAio(values);
        } catch (InterruptedException e) {
            throw new NeciRuntimeException("Error on Aio openning.");
        }
    }

    private void readSetTran(int c) throws IOException {
        List<String> left = new ArrayList<String>();
        List<String> right = new ArrayList<String>();
        int layer = values[c].getLayer();
        String parent = values[c].getParentName();
        if (currentLayer > layer) {
            for (int i = currentLayer; i > layer; i--) {
                left.add(currentParent);
                int col = columnsByName.get(currentParent);
                currentParent = values[col].getParentName();
            }
            currentLayer = layer;
        }
        if (layer > currentLayer) {
            for (int i = layer; i > currentLayer; i--) {
                right.add(parent);
                int col = columnsByName.get(parent);
                parent = values[col].getParentName();
            }
            layer = currentLayer;
        }
        while (currentParent != null && !currentParent.equals(parent)) {
            left.add(currentParent);
            int l = columnsByName.get(currentParent);
            currentParent = values[l].getParentName();
            right.add(parent);
            int r = columnsByName.get(parent);
            parent = values[r].getParentName();
        }

        for (int i = 0; i < left.size(); i++) {
            String array = left.get(i);
            upTran(array);
            String arr = values[columnsByName.get(array)].getParentName();
            bitSetMap.put(arr, chooseSet.size());
            chooseSet.add(filterSet);
        }

        for (int i = right.size() - 1; i >= 0; i--) {
            String array = right.get(i);
            downTran(array);
            bitSetMap.put(array, chooseSet.size());
            BitSet f = filterSetMap.get(array);
            if (f != null) {
                filterSet.and(f);
            }
            chooseSet.add(filterSet);
        }
        currentLayer = values[c].getLayer();
        currentParent = values[c].getParentName();
    }

    public void createFilterRead() throws IOException {
        if (noFilters)
            createRead(defaultMax);
        else
            createFilterRead(defaultMax);
    }

    @SuppressWarnings("static-access")
    public void createFilterRead(int max) throws IOException {
        /*long begin = System.currentTimeMillis();*/
        assert (!noFilters);
        this.defaultMax = max;
        reader.getBlockManager().setSkip(reader.getBlockManager().SKIPPING_MODE);
        // We move the triggers after the bitsets have been generated for each column.
        /*boolean[] intended = new boolean[values.length];
        for (int i = 0; i < readNO.length; i++) {
            if (!values[readNO[i]].isArray()) {
                intended[readNO[i]] = true;
            }
        }
        reader.getBlockManager().trigger(intended);
        for (int i = 0; i < readNO.length; i++) {
            //            values[readNO[i]].createTime();
            //            values[readNO[i]].createSeekBlock();
            values[readNO[i]].create();
        }*/
        //column.getBlockManager().trigger(column.metaData.getNumber());
        readValue = new Object[readNO.length][];
        readLength = new HashMap<String, Integer>();
        readImplPri();
        /*System.out.println("\tfilterReader: " + (System.currentTimeMillis() - begin));*/
    }

    public void createRead(int max) throws IOException {
        assert (noFilters);
        this.defaultMax = max;
        this.getBlockManager().setSkip(false);
        boolean[] intended = new boolean[values.length];
        for (int i = 0; i < readNO.length; i++) {
            if (!values[readNO[i]].isArray()) {
                intended[readNO[i]] = true;
            }
        }
        reader.getBlockManager().trigger(intended, null);
        System.out.println(reader.getBlockManager().getSkip());
        for (int i = 0; i < readNO.length; i++) {
            //            values[readNO[i]].createTime();
            //            values[readNO[i]].createSeekBlock();
            values[readNO[i]].create();
        }
        readValue = new Object[readNO.length][];
        readLength = new HashMap<String, Integer>();
        all = values[readNO[0]].getLastRow();
        readImplPriNoFilters();
    }

    private void readImplPriNoFilters() throws IOException {
        if (all == 0)
            return;
        if (all > defaultMax) {
            currentMax = defaultMax;
            readLength.put(readParent, defaultMax);
        } else {
            currentMax = all;
            readLength.put(readParent, all);
        }
        readImplNoFilters();
        all -= readLength.get(readParent);
        readIndex = new int[readNO.length];
    }

    private void readImplNoFilters() throws IOException {
        for (int i = 0; i < readNO.length; i++) {
            currentMax = readLength.get(values[readNO[i]].getParentName());
            readValue[i] = new Object[currentMax];
            if (values[readNO[i]].isArray()) {
                int j = 0;
                int[] lenAndOff = new int[2];
                values[readNO[i]].startRow();
                lenAndOff = values[readNO[i]].nextLengthAndOffset();
                readValue[i][j++] = lenAndOff[0];
                int off = lenAndOff[1];
                while (j < currentMax) {
                    values[readNO[i]].startRow();
                    lenAndOff = values[readNO[i]].nextLengthAndOffset();
                    readValue[i][j++] = lenAndOff[0];
                }
                readLength.put(values[readNO[i]].getName(), lenAndOff[0] + lenAndOff[1] - off);
            } else {
                int j = 0;
                while (j < currentMax) {
                    readValue[i][j++] = values[readNO[i]].next();
                }
            }
        }
    }

    private void readImplPri() throws IOException {
        /*reader.getBlockManager().markBegin = true;*/
        long start = System.currentTimeMillis();
        setStart = new int[readNO.length];
        readSet = new int[readNO.length];
        int layer = values[readNO[0]].getLayer();
        String parent = values[readNO[0]].getParentName();
        if (layer != currentLayer || (parent != null && !currentParent.equals(parent))) {
            readSetTran(readNO[0]);
        }
        all = filterSet.cardinality();
        if (all > defaultMax) {
            readLength.put(readParent, defaultMax);
        } else {
            readLength.put(readParent, all);
        }
        for (int i = 0; i < readNO.length; i++) {
            parent = values[readNO[i]].getParentName();
            Integer set = bitSetMap.get(parent);
            if (set == null) {
                readSetTran(readNO[i]);
                readSet[i] = chooseSet.size() - 1;
            } else {
                readSet[i] = set;
                filterSet = chooseSet.get(set);
                currentParent = parent;
                currentLayer = values[readNO[i]].getLayer();
            }
            setStart[i] = filterSet.nextSetBit(0);
        }
        filterSetMap.clear();
        filterSetMap = null;
        long end = System.currentTimeMillis();
        System.out.println("read set tran time: " + (end - start));

        /*long begin = System.currentTimeMillis();*/
        boolean[] intended = new boolean[values.length];
        BitSet[] masksets = new BitSet[values.length];
        for (int i = 0; i < readNO.length; i++) {
            if (!values[readNO[i]].isArray()) {
                intended[readNO[i]] = true;
                masksets[readNO[i]] = chooseSet.get(readSet[i]);
            }
        }
        reader.getBlockManager().trigger(intended, masksets);
        /*System.out.println("\t\ttrigger: " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();*/
        for (int i = 0; i < readNO.length; i++) {
            //            values[readNO[i]].createTime();
            //            values[readNO[i]].createSeekBlock();
            values[readNO[i]].create();
        }
        /*System.out.println("\t\tcreate: " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();*/

        for (int i = 0; i < readNO.length; i++) {
            filterSet = chooseSet.get(readSet[i]);
            currentParent = values[readNO[i]].getParentName();
            currentLayer = values[readNO[i]].getLayer();
            readPri(i);
        }

        /*System.out.println("\t\treadPri: " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();*/
        all -= readLength.get(readParent);
        readIndex = new int[readNO.length];
        /*System.out.println("\t\tpost: " + (System.currentTimeMillis() - begin));
        reader.getBlockManager().markEnd = true;*/
    }

    private void readImpl() throws IOException {
        if (all == 0)
            return;
        if (all > defaultMax) {
            readLength.put(readParent, defaultMax);
        } else {
            readLength.put(readParent, all);
        }
        for (int i = 0; i < readNO.length; i++) {
            filterSet = chooseSet.get(readSet[i]);
            currentParent = values[readNO[i]].getParentName();
            currentLayer = values[readNO[i]].getLayer();
            readPri(i);
        }
        all -= readLength.get(readParent);
        readIndex = new int[readNO.length];
    }

    private void readPri(int c) throws IOException {
        int length = readLength.get(currentParent);
        readValue[c] = new Object[length];
        /*long begin = System.currentTimeMillis();*/
        if (values[readNO[c]].isArray()) {
            BitSet set = chooseSet.get(readSet[c + 1]);
            int changeArr = 0;
            int in = 0;
            int p = setStart[c];
            //            int q = -1;
            int m = setStart[c + 1];
            //            if (p == 0) {
            //                int[] res = values[readNO[c]].nextLengthAndOffset();
            //                changeArr += res[0];
            //                readValue[c][in] = res[0];
            //                in++;
            //                q = p;
            //                p = filterSet.nextSetBit(1);
            //            }
            while (in < length) {
                //                int[] res;
                //                values[readNO[c]].seek(p - 1);
                //                values[readNO[c]].nextLengthAndOffset();
                //                res = values[readNO[c]].nextLengthAndOffset();
                //                changeArr += res[0];
                //                readValue[c][in] = res[0];
                values[readNO[c]].seek(p);
                values[readNO[c]].startRow();
                int res = values[readNO[c]].nextLength();
                int re = 0;
                while (m != -1 && res > m) {
                    ++re;
                    m = set.nextSetBit(++m);
                }
                changeArr += re;
                readValue[c][in] = re;
                in++;
                p = filterSet.nextSetBit(++p);
            }
            readLength.put(values[readNO[c]].getName(), changeArr);
            setStart[c] = p;
            /*if (!reader.getBlockManager().markEnd)
                System.out.println("\t\t\tarray: " + (System.currentTimeMillis() - begin) + " "
                        + values[readNO[c]].getName() + " " + filterSet.cardinality() + " " + in + " " + length);*/
        } else {
            int in = 0;
            int m = setStart[c];
            while (in < length) {
                values[readNO[c]].seek(m);
                readValue[c][in] = values[readNO[c]].next();
                in++;
                m = filterSet.nextSetBit(++m);
            }
            setStart[c] = m;
            /*if (!reader.getBlockManager().markEnd)
                System.out.println("\t\t\tnormal: " + (System.currentTimeMillis() - begin) + " "
                        + values[readNO[c]].getName() + " " + filterSet.cardinality() + " " + in + " " + length);*/
        }
    }

    public void setFilters(FilterOperator[] filters) {
        this.filters = filters;
    }

    public D next() {
        try {
            if (readIndex[0] == readValue[0].length) {
                if (noFilters)
                    readImplPriNoFilters();
                else
                    readImpl();
            }
            column = 0;
            return (D) read(readSchema);
        } catch (IOException e) {
            throw new NeciRuntimeException(e);
        }
    }

    public boolean hasNext() {
        return readIndex[0] < readValue[0].length || all > 0;
    }

    private Object read(Schema s) throws IOException {
        if (isSimple(s)) {
            return readValue(s, column++);
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
                int length = (int) readValue[column][readIndex[column]++];

                //                values[readNO[column]].startRow();
                //                int[] rr = values[readNO[column]].nextLengthAndOffset();
                //                length = rr[0];
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
                throw new NeciRuntimeException("Unknown schema: " + s);
        }
    }

    private Object readValue(Schema s, int column) throws IOException {
        Object v = readValue[column][readIndex[column]++];

        switch (s.getType()) {
            case ENUM:
                return model.createEnum(s.getEnumSymbols().get((Integer) v), s);
            case FIXED:
                return model.createFixed(null, ((ByteBuffer) v).array(), s);
            case GROUP: {
                if (groupReader == null) {
                    groupReader = new GenericGroupReader();
                }
                return groupReader.readGroup((GroupCore) v, s);
            }
            case UNION:
                if (v instanceof GroupCore) {
                    if (groupReader == null) {
                        groupReader = new GenericGroupReader();
                    }
                    return groupReader.readGroup((GroupCore) v, s);
                }
        }

        return v;
    }

    @Deprecated
    public void create() throws IOException {
        for (BlockColumnValues v : values) {
            v.create();
        }
    }

    @Deprecated
    public void create(int no) throws IOException {
        values[no].create();
    }

    public int getRowCount(int columnNo) {
        return values[columnNo].getLastRow();
    }

    @Override
    public void close() throws IOException {
        try {
            reader.getBlockManager().closeAio();
        } catch (InterruptedException e) {
            throw new NeciRuntimeException("Cannot close aio after " + readIndex[0] + " out of " + readValue[0].length);
        }
        reader.close();
    }
}

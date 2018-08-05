package neci.ncfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import misc.ValueType;
import neci.ncfile.BloomFilter.BloomFilterBuilder;
import neci.ncfile.CachList.FlagData;
import neci.ncfile.base.Schema;
import neci.ncfile.base.Schema.Field;
import neci.ncfile.base.Schema.Type;
import neci.ncfile.generic.GenericData.Record;

public class NestManager {
    private final int blockSize;
    private NestSchema[] schemas;
    private Schema[] keySchemas;
    private Schema[] nestKeySchemas;
    private Schema[] midSchemas;
    private int[][] keyFields;
    private String resultPath;
    private String tmpPath;
    private int layer;
    private int free;
    private int mul;

    private LayerTree[] tree;

    private CachList cach;

    private BloomFilter[] filter;

    private ColumnReader<Record> reader;

    private int tmpMerge;
    private String tmpPid;

    public NestManager(NestSchema[] schemas, String tmppath, String resultPath, int free, int mul, int blockSize)
            throws IOException {
        assert (schemas.length > 1);
        this.blockSize = blockSize;
        this.schemas = schemas;
        this.tmpPath = tmppath;
        this.resultPath = resultPath;
        layer = schemas.length;
        this.free = free;
        this.mul = mul;
        keySchemas = new Schema[layer];
        nestKeySchemas = new Schema[layer];
        midSchemas = new Schema[layer];
        nestKeySchemas[layer - 1] =
                keySchemas[layer - 1] = setSchema(schemas[layer - 1].getSchema(), schemas[layer - 1].getKeyFields());
        for (int i = (layer - 1); i > 0; i--) {
            keySchemas[i - 1] = setSchema(schemas[i - 1].getSchema(), schemas[i - 1].getKeyFields());
            nestKeySchemas[i - 1] = setSchema(keySchemas[i - 1], nestKeySchemas[i]);
        }
        create();
        tmpMerge = 0;
        tmpPid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        System.out.println("PID: " + tmpPid);
    }

    public void create() throws IOException {
        File rf = new File(resultPath + "result.trv");
        if (rf.exists()) {
            reader = new ColumnReader<Record>(rf);
        }
        tree = new LayerTree[layer];
        filter = new BloomFilter[layer];
        keyFields = new int[layer][];
        for (int j = 0; j < layer; j++)
            keyFields[j] = schemas[j].getKeyFields();
        cach = new CachList(keyFields);
        new File(resultPath).mkdirs();
        midSchemas[layer - 1] = schemas[layer - 1].getMidSchema();

        for (int i = layer - 1; i > 0; i--) {
            filter[i] = new BloomFilter(schemas[i].getBloomFile().getAbsolutePath(), schemas[i].getSchema(),
                    schemas[i].getKeyFields());
            schemas[i - 1].setNestedSchema(setSchema(schemas[i - 1].getSchema(), schemas[i].getNestedSchema()));
            midSchemas[i - 1] = setSchema(schemas[i - 1].getMidSchema(), midSchemas[i]);
        }
        filter[0] = new BloomFilter(schemas[0].getBloomFile().getAbsolutePath(), schemas[0].getSchema(),
                schemas[0].getKeyFields());

        tree[0] = new LayerTree(resultPath + schemas[0].getSchema().getName(), schemas[0].getKeyFields(), true, false,
                schemas[1].getKeyFields(), keySchemas[1], null);
        for (int i = 1; i < layer - 1; i++)
            tree[i] = new LayerTree(resultPath + schemas[i].getSchema().getName(), schemas[i].getKeyFields(), false,
                    false, schemas[i + 1].getKeyFields(), keySchemas[i + 1], schemas[i].getOutKeyFields());
        tree[layer - 1] = new LayerTree(resultPath + schemas[layer - 1].getSchema().getName(),
                schemas[layer - 1].getKeyFields(), false, true, null, null, schemas[layer - 1].getOutKeyFields());
    }

    public void close() throws IOException {
        if (!cach.isEmpty())
            merge();
        reader.close();
        for (LayerTree t : tree)
            t.close();
        for (BloomFilter bb : filter)
            bb.close();
    }

    public void openTree() {
        for (LayerTree t : tree)
            t.create();
    }

    public boolean exists(Record r) {
        int le = getLevel(r);
        return (tree[le].findOffset(new KeyofBTree(r, keyFields[le])) != null);
    }

    public Integer getOffset(Record r) {
        int le = getLevel(r);
        return tree[le].findOffset(new KeyofBTree(r, keyFields[le]));
    }

    public Iterator<btree.Btree.Entry<KeyofBTree, String>> createIterator(int le) {
        return tree[le].createIterator();
    }

    public KeyofBTree getUpperKey(Record r) {
        int le = getLevel(r);
        assert (le > 0);
        return tree[le].findBackKey(new KeyofBTree(r, keyFields[le]));
    }

    public List<Record> getForwardKey(Record r) {
        int le = getLevel(r);
        if (le < layer)
            return tree[le].findForwardDiskRecord(new KeyofBTree(r, keyFields[le]));
        else
            return null;
    }

    public void setMax(int max) {
        cach.setMAX(max);
    }

    private Schema setSchema(Schema schema, int[] fields) {
        List<Field> fs = schema.getFields();
        List<Field> keyFields = new ArrayList<Field>();
        for (int m : fields) {
            Field f = fs.get(m);
            keyFields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
        }
        return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, keyFields);
    }

    private Schema setSchema(Schema s1, Schema s2) {
        List<Field> fs = s1.getFields();
        List<Field> newFS = new ArrayList<Field>();
        for (Field f : fs) {
            newFS.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
        }
        newFS.add(new Schema.Field(s2.getName() + "Arr", Schema.createArray(s2), null, null));
        return Schema.createRecord(s1.getName(), s1.getDoc(), s1.getNamespace(), false, newFS);
    }

    public BloomFilterBuilder createBloom(int numElements, int index) throws IOException {
        BloomFilterModel model = BloomCount.computeBloomModel(BloomCount.maxBucketPerElement(numElements), 0.01);
        return filter[index].creatBuilder(numElements, model.getNumHashes(), model.getNumBucketsPerElement());
    }

    public void load() throws IOException {
        assert layer > 1;
        if (layer == 2) {
            dLoad(schemas[1], schemas[0]); //write directly with column-store according to the highest primary key order
        } else {
            prLoad(schemas[layer - 1], schemas[layer - 2]); //read two primary files, write with row-store according to the next nested key order
            for (int i = layer - 2; i > 1; i--) {
                orLoad(schemas[i], schemas[i - 1], i); //read a primary file and a row-store file, write with row store according to the next nested key order
            }
            laLoad(schemas[1], schemas[0]); //read a primary file and a row-store file, write with column-store according to the highest primary key order
        }
        openTree();
    }

    public int getLevel(Record r) {
        String name = r.getSchema().getName();
        int le = 0;
        while (le < layer) {
            if (name.equals(schemas[le].getSchema().getName())) {
                break;
            }
            le++;
        }
        return le;
    }

    public Record search(Record key, Schema valueSchema) throws IOException {
        return search(key, valueSchema, true);
    }

    public void newMap(int merge) {
        try {
            Runtime.getRuntime().exec("jmap -dump:live,file=" + tmpPath + "map/" + merge + ".map " + tmpPid);
        } catch (Throwable e) {
        }
    }

    public int getTmpMerge() {
        return tmpMerge;
    }

    public void merge() throws IOException {
        if (cach.size() == 0)
            return;
        tmpMerge++;
        System.out.println();
        System.out.println("####################" + tmpMerge);
        long start = System.currentTimeMillis();
        cach.mergeCreate();
        int[] number = new int[layer];
        for (int i = 0; i < layer; i++) {
            while (cach.hasNext(i)) {
                Entry<KeyofBTree, FlagData> ne = cach.next(i);
                if (ne.getValue().getFlag() == (byte) 4) {
                    int nest = 1;
                    int place = (int) tree[i].findOffset(ne.getKey());
                    Record r = reader.search(schemas[i].getSchema(), place); //read the record from disk to complement the update-insert(5) record
                    Record exF = cach.extraFind(r, i, false).getData();
                    for (int k = 0; k < r.getSchema().getFields().size(); k++) {
                        Object o = exF.get(k);
                        if (o != null)
                            r.put(k, o);
                    }
                    cach.addToMergeList(place, null, i);
                    for (int j = i; j < (layer - 1); j++) {
                        int p = 0;
                        p += reader.searchArray(0, j, place);
                        place = p;
                        p = 0;
                        for (int m = 0; m < nest; m++) {
                            p += reader.nextArray(j);
                        }
                        nest = p;
                        for (int n = place; n < (place + nest); n++) {
                            cach.addToMergeList(n, null, j);
                        }
                        Record[] records = reader.search(schemas[j].getSchema(), place, nest);
                        for (Record record : records) {
                            FlagData fd = cach.find(record, j, false);
                            if (fd == null)
                                cachOperate(record, (byte) 1, j);
                            else if (fd.getFlag() == (byte) 3) {
                                Record tmp = fd.getData();
                                int ll = tmp.getSchema().getFields().size();
                                for (int mm = 0; mm < ll; mm++) {
                                    if (tmp.get(mm) != null)
                                        record.put(mm, tmp.get(mm));
                                }
                                cachOperate(record, (byte) 1, j);
                            } else {
                                cach.delete(record, j, false);
                                if (fd.getFlag() == (byte) 4) {
                                    FlagData ff = cach.extraFind(record, j, false);
                                    cach.extraDelete(record, j, false);
                                    int len = record.getSchema().getFields().size();
                                    for (int k = 0; k < len; k++) {
                                        Object o = ff.getData().get(k);
                                        if (o != null)
                                            record.put(k, o);
                                    }
                                    cachOperate(record, (byte) 1, j);
                                }
                            }
                        }
                    }
                } else if (ne.getValue().getFlag() == (byte) 2) {
                    number[i]--;
                    cach.addToMergeList(tree[i].findOffset(ne.getKey()).intValue(), null, i); //find the place in in disk, and add to mergeList with null value(means delete)
                } else {
                    if (ne.getValue().getFlag() == (byte) 1)
                        number[i]++;
                    NestCombKey fk = findKey(ne.getValue().getData(), i);
                    cach.addToSortList(fk, ne.getValue(), i);
                }
            }
            if (i > 0) {
                while (cach.extraHasNext(i)) {
                    Entry<KeyofBTree, FlagData> ne = cach.extraNext(i);
                    NestCombKey fk = findKey(ne.getValue().getData(), i);
                    cach.addToSortList(fk, ne.getValue(), i);
                }
            }
            cach.sortSortList(i);
            cach.sortMergeList(i);
        }
        cach.hashClear();
        long t1 = System.currentTimeMillis();
        System.out.println("hash -> sortList(mergeList): " + (t1 - start));
        sortToMerge();
        long t2 = System.currentTimeMillis();
        System.out.println("sortList -> mergeList: " + (t2 - t1));
        mergeWrite();
        long t3 = System.currentTimeMillis();
        System.out.println("merge write: " + (t3 - t2));
        updateTreeBloom();
        long end = System.currentTimeMillis();
        System.out.println("update Btree and bloom filter: " + (end - t3));
        System.out.println("sum time: " + (end - start));
        openTree();
    }

    public static void shDelete(String path) {
        try {
            System.out.println("Delete on: " + path);
            File file = new File(path);
            if (file.isDirectory()) {
                FileUtils.deleteDirectory(new File(path));
            } else {
                file.delete();
            }
        } catch (IOException x) {
            x.printStackTrace();
        }
    }

    public void arrayTest() throws IOException {
        reader.create();
        int okLen = 0;
        int lkLen = 0;
        int len1 = reader.getRowCount(0);
        int len2 = reader.getRowCount(9);
        int len3 = reader.getRowCount(19);

        int i = 0;
        int aL1 = reader.getRowCount(8);
        System.out.println(aL1);
        while (i < aL1) {
            okLen += reader.nextArray(0);
            i++;
        }
        i = 0;
        int aL2 = reader.getRowCount(18);
        System.out.println(aL2);
        while (i < aL2) {
            lkLen += reader.nextArray(1);
            i++;
        }
        System.out.println(len1);
        System.out.println(len2 + " VS " + okLen);
        System.out.println(len3 + " VS " + lkLen);
    }

    public void sortToMerge() throws IOException {
        cach.createSortIterator();
        Integer[] place = new Integer[layer];
        for (int i = 0; i < layer; i++)
            place[i] = new Integer(0);
        Entry<NestCombKey, FlagData>[] sort = new Entry[layer];
        for (int i = 0; i < layer; i++) {
            if (cach.hasNextSort(i))
                sort[i] = cach.nextSort(i);
        }
        reader.create();
        reader.createSchema(nestKeySchemas[0]);

        Integer[] mergeIndex = new Integer[layer];
        for (int i = 0; i < layer; i++)
            mergeIndex[i] = new Integer(0);
        int[] mergeLen = new int[layer];
        for (int i = 0; i < mergeLen.length; i++) {
            mergeLen[i] = cach.getMergeLength(i);
        }

        Integer[] count = new Integer[layer - 1];
        RandomAccessFile[] arrayColumns = new RandomAccessFile[layer - 1];
        for (int i = 0; i < layer - 1; i++) {
            count[i] = new Integer(0);
            arrayColumns[i] = new RandomAccessFile((tmpPath + "array" + i), "rw");
            arrayColumns[i].seek(4);
        }

        while (reader.hasNext()) {
            Record nestKey = reader.next();
            List<NestCombKey>[] keys = new List[layer];
            for (int i = 0; i < layer; i++) {
                keys[i] = new ArrayList<NestCombKey>();
            }
            keys[0].add(new NestCombKey(new KeyofBTree(nestKey, keyFields[0].length)));
            comNestKey(nestKey.get(keyFields[0].length), 1, keys, keys[0].get(0));

            List<NestCombKey> kks = new ArrayList<NestCombKey>();
            while (sort[0] != null && sort[0].getKey().compareTo(keys[0].get(0)) < 0) {
                cach.addToMergeList(place[0].intValue(), sort[0].getValue(), 0);
                kks.add(sort[0].getKey());
                if (cach.hasNextSort(0))
                    sort[0] = cach.nextSort(0);
                else {
                    sort[0] = null;
                    cach.sortClear(0);
                }
            }
            if (sort[0] != null && sort[0].getKey().compareTo(keys[0].get(0)) == 0) {
                cach.addToMergeList(place[0].intValue(), sort[0].getValue(), 0);
                kks.add(sort[0].getKey());
                if (cach.hasNextSort(0))
                    sort[0] = cach.nextSort(0);
                else {
                    sort[0] = null;
                    cach.sortClear(0);
                }
            } else if (mergeIndex[0].intValue() < mergeLen[0]
                    && cach.getMergePlace(0, mergeIndex[0]) == place[0].intValue()) {
                mergeIndex[0] = mergeIndex[0].intValue() + 1;
            } else {
                kks.add(keys[0].get(0));
            }
            place[0] = place[0].intValue() + 1;
            if (kks.isEmpty())
                sortToMerge(keys, 1, count, place, mergeIndex, mergeLen);
            else
                sortToMerge(sort, keys, 1, arrayColumns, count, kks, place, mergeIndex, mergeLen);
        }
        while (sort[0] != null) {
            List<NestCombKey> kks = new ArrayList<NestCombKey>();
            cach.addToMergeList(place[0].intValue(), sort[0].getValue(), 0);
            kks.add(sort[0].getKey());
            if (cach.hasNextSort(0))
                sort[0] = cach.nextSort(0);
            else {
                sort[0] = null;
                cach.sortClear(0);
            }
            sortToMerge(sort, 1, arrayColumns, count, kks, place);
            cach.sortClear(0);
        }
        for (int i = 0; i < layer; i++)
            cach.sortMergeList(i);
        cach.sortClear();
        for (int i = 0; i < arrayColumns.length; i++) {
            arrayColumns[i].seek(0);
            arrayColumns[i].writeInt(count[i]);
            arrayColumns[i].close();
        }
    }

    private void sortToMerge(Entry<NestCombKey, FlagData>[] sort, int le, RandomAccessFile[] arrayColumns,
            Integer[] count, List<NestCombKey> kks, Integer[] place) throws IOException {
        assert (le < layer);
        int[] cc = new int[kks.size()];
        int kIn = 0;
        List<NestCombKey> kkks = new ArrayList<NestCombKey>();
        while (sort[le] != null && kks.get(kks.size() - 1).compareTo(sort[le].getKey(), le - 1) >= 0) {
            cach.addToMergeList(place[le].intValue(), sort[le].getValue(), le);
            kkks.add(sort[le].getKey());
            while (kks.get(kIn).compareTo(sort[le].getKey(), le - 1) < 0)
                kIn++;
            if (kks.get(kIn).compareTo(sort[le].getKey(), le - 1) == 0) {
                cc[kIn]++;
            } else {
                System.out.println("!!!!!!!!!!!error:" + sort[le].getKey().getKey(0).values[0]);
            }
            if (cach.hasNextSort(le))
                sort[le] = cach.nextSort(le);
            else {
                sort[le] = null;
                cach.sortClear(le);
            }
        }
        for (int c : cc) {
            arrayColumns[le - 1].writeInt(c);
        }
        count[le - 1] = count[le - 1].intValue() + cc.length;
        if (le < (layer - 1))
            sortToMerge(sort, (le + 1), arrayColumns, count, kkks, place);
    }

    private void sortToMerge(Entry<NestCombKey, FlagData>[] sort, List<NestCombKey>[] keys, int le,
            RandomAccessFile[] arrayColumns, Integer[] count, List<NestCombKey> kks, Integer[] place,
            Integer[] mergeIndex, int[] mergeLen) throws IOException {
        assert (le < layer);
        int[] cc = new int[kks.size()];
        int kIn = 0;
        List<NestCombKey> kkks = new ArrayList<NestCombKey>();
        for (NestCombKey o : keys[le]) {
            while (sort[le] != null && sort[le].getKey().compareTo(o) < 0) {
                cach.addToMergeList(place[le].intValue(), sort[le].getValue(), le);
                kkks.add(sort[le].getKey());
                while (kks.get(kIn).compareTo(sort[le].getKey(), le) < 0)
                    kIn++;
                if (kks.get(kIn).compareTo(sort[le].getKey(), le) == 0) {
                    cc[kIn]++;
                } else {
                    System.out.println("!!!!!!!!!!!error:" + sort[le].getKey().getKey(0).values[0]);
                }
                if (cach.hasNextSort(le))
                    sort[le] = cach.nextSort(le);
                else {
                    sort[le] = null;
                    cach.sortClear(le);
                }
            }
            if (sort[le] != null && sort[le].getKey().compareTo(o) == 0) {
                cach.addToMergeList(place[le].intValue(), sort[le].getValue(), le);
                kkks.add(sort[le].getKey());
                while (kks.get(kIn).compareTo(sort[le].getKey(), le) < 0)
                    kIn++;
                if (kks.get(kIn).compareTo(sort[le].getKey(), le) == 0) {
                    cc[kIn]++;
                } else {
                    System.out.println("!!!!!!!!!!!error:" + sort[le].getKey().getKey(0).values[0]);
                }
                if (cach.hasNextSort(le))
                    sort[le] = cach.nextSort(le);
                else {
                    sort[le] = null;
                    cach.sortClear(le);
                }
            } else if (mergeIndex[le].intValue() < mergeLen[le]
                    && cach.getMergePlace(le, mergeIndex[le]) == place[le].intValue()) {
                mergeIndex[le] = mergeIndex[le].intValue() + 1;
            } else {
                kkks.add(o);
                while (kks.get(kIn).compareTo(o, le) < 0)
                    kIn++;
                if (kks.get(kIn).compareTo(o, le) == 0) {
                    cc[kIn]++;
                } else {
                    System.out.println("!!!!!!!!!!!error:" + o.getKey(0).values[0]);
                }
            }
            place[le] = place[le].intValue() + 1;
        }

        while (sort[le] != null && sort[le].getKey().compareTo(kks.get(kks.size() - 1), le) <= 0) {
            cach.addToMergeList(place[le].intValue(), sort[le].getValue(), le);
            kkks.add(sort[le].getKey());
            while (kks.get(kIn).compareTo(sort[le].getKey(), le) < 0)
                kIn++;
            if (kks.get(kIn).compareTo(sort[le].getKey(), le) == 0) {
                cc[kIn]++;
            } else {
                System.out.println("!!!!!!!!!!!error:" + sort[le].getKey().getKey(0).values[0]);
            }
            if (cach.hasNextSort(le))
                sort[le] = cach.nextSort(le);
            else {
                sort[le] = null;
                cach.sortClear(le);
            }
        }

        if (!kks.isEmpty()) {
            for (int c : cc) {
                arrayColumns[le - 1].writeInt(c);
            }
            count[le - 1] = count[le - 1].intValue() + cc.length;
        }

        if (le < (layer - 1)) {
            if (kkks.isEmpty())
                sortToMerge(keys, (le + 1), count, place, mergeIndex, mergeLen);
            else
                sortToMerge(sort, keys, (le + 1), arrayColumns, count, kkks, place, mergeIndex, mergeLen);
        }
    }

    private void sortToMerge(List<NestCombKey>[] keys, int le, Integer[] count, Integer[] place, Integer[] mergeIndex,
            int[] mergeLen) {
        assert (le < layer);
        for (NestCombKey o : keys[le]) {
            if (mergeIndex[le].intValue() < mergeLen[le]
                    && cach.getMergePlace(le, mergeIndex[le]) == place[le].intValue()) {
                mergeIndex[le] = mergeIndex[le].intValue() + 1;
            } else {
                System.out.println("!!!!delete error: no that delete:" + o.getKey(0).values[0]);
            }
            place[le] = place[le].intValue() + 1;
        }
        if (le < (layer - 1)) {
            sortToMerge(keys, (le + 1), count, place, mergeIndex, mergeLen);
        }
    }

    public void mergeWrite() throws IOException {
        reader.create();
        ValueType[] types = reader.getTypes();
        AvroColumnWriter writer = new AvroColumnWriter(schemas[0].getNestedSchema(), tmpPath + "result.tmp", blockSize);
        cach.mergeWriteCreate();
        int i = 0;
        int a = 0;
        int array = 0;
        for (ValueType type : types) {
            if (type == ValueType.NULL) {
                RandomAccessFile in = new RandomAccessFile((tmpPath + "array" + a), "rw");
                int len = in.readInt();
                int tmp = 0;
                for (int k = 0; k < len; k++) {
                    tmp += in.readInt();
                    writer.writeArrayColumn(i, tmp); //write array column incremently
                }
                writer.flush(i);
                in.close();
                in = null;
                shDelete(tmpPath + "array" + a);
                a++;
                i++;
                array = i;
            } else {
                int len = reader.getRowCount(i);
                Entry<Integer, FlagData> en = cach.mergeNext(a);
                int k = 0;
                while (en != null && k < len) {
                    while (k < en.getKey()) {
                        writer.writeColumn(i, reader.nextValue(i));
                        k++;
                    }
                    if (en.getValue() == null) {
                        reader.nextValue(i);
                        k++;
                        en = cach.mergeNext(a);
                    } else {
                        byte f = en.getValue().getFlag();
                        if (f == (byte) 3) {
                            Object v = reader.nextValue(i);
                            k++;
                            Object up = en.getValue().getData().get(i - array);
                            if (up != null)
                                v = up;
                            writer.writeColumn(i, v);
                            en = cach.mergeNext(a);
                        } else {
                            writer.writeColumn(i, en.getValue().getData().get(i - array));
                            en = cach.mergeNext(a);
                        }
                    }
                }
                while (en != null) {
                    writer.writeColumn(i, en.getValue().getData().get(i - array));
                    en = cach.mergeNext(a);
                }
                while (k < len) {
                    writer.writeColumn(i, reader.nextValue(i));
                    k++;
                }
                writer.flush(i);
                cach.mergeWriteCreate();
                i++;
            }
        }
        cach.clear();
        reader.close();
        writer.close();
        reader = null;
        writer = null;
        new File(resultPath + "result.trv").delete();
        new File(resultPath + "result.head").delete();
        new File(tmpPath + "result.tmp").renameTo(new File(resultPath + "result.trv"));
        new File(tmpPath + "result.head").renameTo(new File(resultPath + "result.head"));
        System.gc();
        reader = new ColumnReader<Record>(new File(resultPath + "result.trv"));
    }

    public void updateTreeBloom() throws IOException {
        reader.createSchema(nestKeySchemas[0]);
        BloomFilterBuilder[] builder = new BloomFilterBuilder[layer];
        for (int i = 0; i < layer; i++) {
            tree[i].createMerge(reader.getLevelRowCount(i) / 4000);
            filter[i].cover();
            builder[i] = createBloom(reader.getLevelRowCount(i), i);
        }
        Integer[] index = new Integer[layer];
        for (int m = 0; m < layer; m++)
            index[m] = 0;
        while (reader.hasNext()) {
            Record record = reader.next();
            updateTreeBloom(0, null, index, record, builder);
        }
        for (int i = 0; i < layer; i++) {
            tree[i].write();
            builder[i].write();
        }
    }

    private void updateTreeBloom(int le, KeyofBTree backKey, Integer[] index, Record record,
            BloomFilterBuilder[] builder) throws IOException {
        KeyofBTree k = new KeyofBTree(record, keyFields[le].length);
        builder[le].add(k);
        if (le < (layer - 1)) {
            List<Record> rs = (List<Record>) (record.get(keyFields[le].length));
            List<Record> tm = new ArrayList<Record>();
            for (Record r : rs) {
                Record m = new Record(keySchemas[le + 1]);
                for (int i = 0; i < keyFields[le + 1].length; i++)
                    m.put(i, r.get(i));
                tm.add(m);
            }
            tree[le].put(k, index[le]++, backKey, tm);
            for (Record r : rs)
                updateTreeBloom(le + 1, k, index, r, builder);
        } else {
            tree[le].put(k, index[le]++, backKey, null);
        }
    }

    private void updateTree(int le, KeyofBTree backKey, Integer[] index, Record record) {
        KeyofBTree k = new KeyofBTree(record, keyFields[le].length);
        if (le < (layer - 1)) {
            List<Record> rs = (List<Record>) (record.get(keyFields[le].length));
            List<Record> tm = new ArrayList<Record>();
            for (Record r : rs) {
                Record m = new Record(keySchemas[le + 1]);
                for (int i = 0; i < keyFields[le + 1].length; i++)
                    m.put(i, r.get(i));
                tm.add(m);
            }
            tree[le].put(k, index[le]++, backKey, tm);
            for (Record r : rs)
                updateTree(le + 1, k, index, r);
        } else {
            //System.out.println("\t" + k.toString() + ":" + backKey.toString());
            tree[le].put(k, index[le]++, backKey, null);
        }
    }

    /*
     * give a record in forwardTree,
     * according to this, compute the nestkeys in all layers with the recursion method
     */
    private void comNestKey(Object data, int le, List<NestCombKey>[] keys, NestCombKey upperkey) {
        assert (le < layer);
        if (data == null)
            return;
        List<Record> nest = (List<Record>) data;
        if (le < (layer - 1)) {
            for (Record tm : nest) {
                KeyofBTree key = new KeyofBTree(tm, keyFields[le].length);
                keys[le].add(new NestCombKey(upperkey, key));
                comNestKey(tm.get(keyFields[le].length), le + 1, keys, new NestCombKey(upperkey, key));
            }
        } else {
            for (Record tm : nest) {
                KeyofBTree key = new KeyofBTree(tm, keyFields[le].length);
                keys[le].add(new NestCombKey(upperkey, key));
            }
        }
    }

    /*
     * first use backTree find the up layer key,
     * then find the sortList in cach with binary search,
     * if exists return the nest combkey, else use backTree find the up layer key until the nest combkey is found.
     * this return is not always the newest nest combkey.
     */
    private NestCombKey findSortKey(KeyofBTree key, int le) {
        if (le == 0)
            return new NestCombKey(new KeyofBTree[] { key });
        KeyofBTree[] keys = new KeyofBTree[le + 1];
        keys[le] = key;
        KeyofBTree fk = tree[le].findBackKey(key);
        keys[le - 1] = fk;
        for (int i = le - 1; i > 0; i++) {
            NestCombKey fks = cach.findSort(keys[i], i);
            if (fks != null) {
                int j = 0;
                for (KeyofBTree kk : fks.keys) {
                    keys[j++] = kk;
                }
                break;
            }
            keys[i - 1] = tree[i].findBackKey(keys[i]);
        }
        return new NestCombKey(keys);
    }

    /*
     * find the up comkkey with backTree,
     * return the disk nest combkey.
     */
    private NestCombKey findBackKey(KeyofBTree key, int le) {
        if (le == 0)
            return new NestCombKey(new KeyofBTree[] { key });
        KeyofBTree[] keys = new KeyofBTree[le + 1];
        keys[le] = key;
        for (int i = le; i > 0; i++) {
            keys[i - 1] = tree[i].findBackKey(keys[i]);
        }
        return new NestCombKey(keys);
    }

    /*
     * first use backTree find the up layer key,
     * then find the up layer cach's hash, find whether the foreign key is changed,
     * use the newer foreign key,
     * return the newest nest combkey.
     */
    private NestCombKey findKey(KeyofBTree key, int le) {
        if (le == 0)
            return new NestCombKey(new KeyofBTree[] { key });
        KeyofBTree[] keys = new KeyofBTree[le + 1];
        keys[le] = key;
        KeyofBTree fk = tree[le].findBackKey(key);
        keys[le - 1] = fk;
        for (int i = le - 1; i > 0; i++) {
            FlagData fd = cach.find(keys[i], i);
            if (fd != null) {
                byte b = fd.getFlag();
                if (b == (byte) 1) {
                    keys[i - 1] = new KeyofBTree(fd.getData(), schemas[i].getOutKeyFields());
                } else if (b == (byte) 4) {
                    fd = cach.extraFind(keys[i], i);
                    keys[i - 1] = new KeyofBTree(fd.getData(), schemas[i].getOutKeyFields());
                }
                break;
            }
            keys[i - 1] = tree[i].findBackKey(keys[i]);
        }
        return new NestCombKey(keys);
    }

    /*
     * this record is a full data, return newest nest combkey.
     * compares to findKey(CombKey key, int le),
     * this function adds the operation that judge whether the record itself contains the foreignKey,
     * decides whether or not finding the backTree.
     */
    private NestCombKey findKey(Record data, int le) {
        if (le == 0)
            return new NestCombKey(new KeyofBTree[] { new KeyofBTree(data, schemas[0].getKeyFields()) });
        KeyofBTree[] keys = new KeyofBTree[le + 1];
        keys[le] = new KeyofBTree(data, schemas[le].getKeyFields());
        keys[le - 1] = isNullKey(data, schemas[le].getOutKeyFields())
                ? findForeignKey(new KeyofBTree(data, schemas[le].getKeyFields()), le)
                : new KeyofBTree(data, schemas[le].getOutKeyFields());
        for (int i = le - 1; i > 0; i--) {
            FlagData fd = cach.find(keys[i], i);
            if (fd != null) {
                byte b = fd.getFlag();
                if (b == (byte) 1) {
                    keys[i - 1] = new KeyofBTree(fd.getData(), schemas[i].getOutKeyFields());
                } else if (b == (byte) 4) {
                    fd = cach.extraFind(keys[i], i);
                    keys[i - 1] = new KeyofBTree(fd.getData(), schemas[i].getOutKeyFields());
                }
                break;
            }
            keys[i - 1] = tree[i].findBackKey(keys[i]);
        }
        return new NestCombKey(keys);
    }

    private KeyofBTree findForeignKey(KeyofBTree key, int le) {
        assert (le > 0);
        FlagData fd = cach.find(key, le);
        if (fd != null) {
            byte b = fd.getFlag();
            if (b == (byte) 2)
                return null;
            if (b == (byte) 1)
                return new KeyofBTree(fd.getData(), schemas[le].getOutKeyFields());
            if (b == (byte) 4)
                return new KeyofBTree(cach.extraFind(key, le).getData(), schemas[le].getOutKeyFields());
            if (b == (byte) 3)
                if (!isNullKey(fd.getData(), schemas[le].getOutKeyFields()))
                    return new KeyofBTree(fd.getData(), schemas[le].getOutKeyFields());
        }
        return tree[le].findBackKey(key);
    }

    public Record search(Record key, Schema valueSchema, boolean isKey) throws IOException {
        assert key.getSchema().getName().equals(valueSchema.getName());
        int le = getLevel(key);
        FlagData fd = cach.find(key, le, isKey);
        if (fd == null) {
            return diskSearch(key, valueSchema, isKey, le);
        } else {
            byte b = fd.getFlag();
            if (b == (byte) 1)
                return fd.getData();
            if (b == (byte) 2)
                return null;
            Record res = diskSearch(key, valueSchema, isKey, le);
            List<Field> fs = valueSchema.getFields();
            if (b == (byte) 4)
                fd = cach.extraFind(key, le, isKey);
            for (Field f : fs) {
                Object v = fd.getData().get(f.name());
                if (v != null)
                    res.put(f.name(), v);
            }
            return res;
        }
    }

    public Record diskSearch(Record key, Schema valueSchema, boolean isKey, int le) throws IOException {
        if (!filter[le].isActivated()) {
            filter[le].activate();
        }
        if (filter[le].contains(key, isKey, new long[2])) {
            Object v;
            if ((v = tree[le].findOffset(key, isKey)) != null) {
                if (reader == null) {
                    reader = new ColumnReader<Record>(new File(resultPath + "result.trv"));
                }
                int row = (Integer) v;
                Record res = reader.search(valueSchema, row);
                //System.out.println(res);
                return res;
            }
        }
        System.out.println("The key is not existed:" + key);
        return null;
    }

    public void insert(Record data) throws IOException {
        int le = getLevel(data);
        FlagData fd = cach.find(data, le, false);
        if (fd == null) {
            if (!filter[le].isActivated()) {
                filter[le].activate();
            }
            if (filter[le].contains(data, false, new long[2])) {
                Object v;
                if ((v = tree[le].findOffset(data, false)) != null) {
                    //                    System.out.println("insert disk exists error: " + data);
                    return;
                } else {
                    if (insertLegal(data, le))
                        insertToCach(data, le);
                }
            } else {
                if (insertLegal(data, le))
                    insertToCach(data, le);
            }
        } else {
            if (fd.getFlag() == (byte) 2) {
                if (fd.getLevel() == le) {
                    deleteAndInsert(fd, data, le);
                } else {
                    System.out.println("insert illeagal: " + data);//insert illeagal
                    return;
                }
            } else {
                //                System.out.println("insert memory exists error: " + data);
                return;
            }
        }
        if (cach.isFull())
            merge();
    }

    public void upsert(Record data) throws IOException {
        int le = getLevel(data);
        FlagData fd = cach.find(data, le, false);
        if (fd == null) {
            if (!filter[le].isActivated()) {
                filter[le].activate();
            }
            if (filter[le].contains(data, false, new long[2])) {
                Object v;
                if ((v = tree[le].findOffset(data, false)) != null) {
                    updateToCach(data, le);
                } else {
                    if (insertLegal(data, le))
                        insertToCach(data, le);
                }
            } else {
                if (insertLegal(data, le))
                    insertToCach(data, le);
            }
        } else {
            if (fd.getFlag() == (byte) 2) {
                if (fd.getLevel() == le) {
                    deleteAndInsert(fd, data, le);
                } else {
                    System.out.println("insert illeagal: " + data);//insert illeagal
                    return;
                }
            } else {
                upsertAndUpdate(fd, data, le);
            }
        }
        if (cach.isFull())
            merge();
    }

    public void update(Record data) throws IOException {
        int le = getLevel(data);
        FlagData fd = cach.find(data, le, false);
        if (fd == null) {
            if (!filter[le].isActivated()) {
                filter[le].activate();
            }
            if (filter[le].contains(data, false, new long[2])) {
                Object v;
                if ((v = tree[le].findOffset(data, false)) != null) {
                    updateToCach(data, le);
                } else {
                    //                    System.out.println("update disk not exists error: " + data);
                    return;
                }
            } else {
                //                System.out.println("update disk not exists error: " + data);
                return;
            }
        } else {
            if (fd.getFlag() == (byte) 2) {
                //                System.out.println("update memory delete error: " + data);
                return;
            } else {
                upsertAndUpdate(fd, data, le);
            }
        }
        if (cach.isFull())
            merge();
    }

    public void delete(Record data) throws IOException {
        int le = getLevel(data);
        //        System.out.println("delete layer" + le);
        FlagData fd = cach.find(data, le, false);
        if (fd == null) {
            if (!filter[le].isActivated()) {
                filter[le].activate();
            }
            if (filter[le].contains(data, false, new long[2])) {
                if (tree[le].findOffset(data, false) != null) {
                    deleteToCach(data, le);
                } else {
                    //                    System.out.println("delete disk not exists error: " + data);
                    return;
                }
            } else {
                //                System.out.println("delete disk not exists error: " + data);
                return;
            }
        } else {
            if (fd.getFlag() == (byte) 2) {
                //                System.out.println("delete memory delete error: " + data);
                return;
            } else {
                upsertAndDelete(fd, data, le);
            }
        }
        if (cach.isFull())
            merge();
    }

    private void deleteAndInsert(FlagData fd, Record data, int le) throws IOException {
        cach.delete(data, le, false);
        updateToCach(data, le);
    }

    private boolean isNullKey(Record data, int[] fields) {
        for (int i = 0; i < fields.length; i++) {
            if (data.get(i) == null)
                return true;
        }
        return false;
    }

    private void upsertAndUpdate(FlagData fd, Record data, int le) throws IOException {
        byte b = fd.getFlag();
        if (b == (byte) 1) {
            Record d = fd.getData();
            for (int i = 0; i < schemas[le].getSchema().getFields().size(); i++) {
                if (data.get(i) == null)
                    data.put(i, d.get(i));
            }
            insertToCach(data, le);
        }
        if (b == (byte) 3) {
            updateToCach(data, le);
        }
        if (b == (byte) 4) {
            if (isNullKey(data, schemas[le].getOutKeyFields())) {
                Record rr = cach.extraFind(data, le, false).getData();
                for (int i = 0; i < rr.getSchema().getFields().size(); i++) {
                    if (data.get(i) != null) {
                        rr.put(i, data.get(i));
                    }
                }
                extraCachOperate(rr, (byte) 5, le);
                return;
            }

            KeyofBTree uppNew = new KeyofBTree(data, schemas[le].getOutKeyFields());
            Record rr = cach.extraFind(data, le, false).getData();
            KeyofBTree upper = new KeyofBTree(rr, schemas[le].getOutKeyFields());
            KeyofBTree key = new KeyofBTree(data, keyFields[le]);
            Record caR = fd.getData();
            KeyofBTree uppDisk = new KeyofBTree(caR, schemas[le].getOutKeyFields());
            if (uppNew.equals(uppDisk)) {
                tree[le - 1].deleteForward(uppDisk, key);
                tree[le - 1].deleteForward(upper, key);
                cach.extraDelete(data, le, false);
                cachOperate(data, (byte) 3, le);
            } else {
                if (!uppNew.equals(upper)) {
                    tree[le - 1].deleteForward(upper, key);
                    tree[le - 1].deleteForward(uppNew, key);
                }
                for (int i = 0; i < rr.getSchema().getFields().size(); i++) {
                    if (data.get(i) != null) {
                        rr.put(i, data.get(i));
                    }
                }
                extraCachOperate(rr, (byte) 5, le);
            }
        }
    }

    private void upsertAndDelete(FlagData fd, Record data, int le) throws IOException {
        byte b = fd.getFlag();
        if (b == (byte) 4) {
            cach.extraDelete(data, le, false);
        }
        deleteToCach(data, le);
    }

    private List<Record[]> split(Record data, int le) {
        assert (le > 0);
        int i = data.getSchema().getFields().size();
        List<Record> rs = (List<Record>) data.get(i - 1);
        List<Record[]> res = new ArrayList<Record[]>();
        if (le == 1) {
            for (Record r : rs) {
                Record tm = new Record(data.getSchema());
                for (int k = 0; k < (i - 1); k++) {
                    tm.put(k, data.get(k));
                }
                tm.put((i - 1), new ArrayList<Record>().add(r));
                res.add(new Record[] { tm, r });
            }
        } else {
            List<Record[]> mm = split(rs.get(0), (le - 1));
            for (Record[] r : mm) {
                Record tm = new Record(data.getSchema());
                for (int k = 0; k < (i - 1); k++) {
                    tm.put(k, data.get(k));
                }
                tm.put((i - 1), new ArrayList<Record>().add(r[0]));
                res.add(new Record[] { tm, r[1] });
            }
        }
        return res;
    }

    private boolean insertLegal(Record data, int le) throws IOException {
        if (le == 0) {
            return true;
        }
        FlagData fd = cach.find(data, schemas[le].getOutKeyFields(), (le - 1));
        if (fd == null) {
            if (!filter[le - 1].isActivated()) {
                filter[le - 1].activate();
            }
            return (filter[le - 1].contains(data, schemas[le].getOutKeyFields(), new long[2])
                    && tree[le - 1].findOffset(data, schemas[le].getOutKeyFields()) != null);
        }
        return (fd.getFlag() != (byte) 2);
    }

    private int[] comFields(int len) {
        int[] res = new int[len];
        for (int i = 0; i < len; i++) {
            res[i] = i;
        }
        return res;
    }

    private void setKey(Record to, int[] f1, Record from, int[] f2) {
        assert (f1.length == f2.length);
        for (int i = 0; i < f1.length; i++) {
            to.put(f1[i], from.get(f2[i]));
        }
        int i = to.getSchema().getFields().size();
        List<Record> arr = new ArrayList<Record>();
        arr.add(from);
        to.put((i - 1), arr);
    }

    private void setKey(Record to, Record from, int[] f2) {
        for (int i = 0; i < f2.length; i++) {
            to.put(i, from.get(f2[i]));
        }
    }

    private void setKey(Record to, int[] f1, Record from) {
        for (int i = 0; i < f1.length; i++) {
            to.put(f1[i], from.get(i));
        }
    }

    private void setKey(Record to, int[] f1, Record key, Record from) {
        int len = key.getSchema().getFields().size();
        assert (f1.length == len);
        for (int i = 0; i < len; i++) {
            to.put(f1[i], key.get(i));
        }
        int i = to.getSchema().getFields().size();
        List<Record> arr = new ArrayList<Record>();
        arr.add(from);
        to.put((i - 1), arr);
    }

    public void addToForward(Record data, int le, boolean isKey) {
        if (le < (layer - 1))
            tree[le].insertForward(data, isKey);
        if (le > 0) {
            KeyofBTree key = isKey ? new KeyofBTree(data, keyFields[le].length) : new KeyofBTree(data, keyFields[le]);
            KeyofBTree upper = findForeignKey(key, le);
            tree[le - 1].insertForward(upper, key);
        }
    }

    public void deleteFromForward(Record data, int le) {

    }

    private void insertToCach(Record data, int le) {
        cachOperate(data, (byte) 1, le);
        if (le < (layer - 1))
            tree[le].insertForward(data, false);
        if (le > 0) {
            KeyofBTree key = new KeyofBTree(data, keyFields[le]);
            KeyofBTree upper = new KeyofBTree(data, schemas[le].getOutKeyFields());
            tree[le - 1].insertForward(upper, key);
        }
    }

    private void updateToCach(Record data, int le) throws IOException {
        if (le > 0 && tree[le].hasBackTree) {
            KeyofBTree upper = findForeignKey(new KeyofBTree(data, keyFields[le]), le);
            int[] fields = schemas[le].getOutKeyFields();
            if (!isNullKey(data, fields)) {
                if (!upper.equals(new KeyofBTree(data, fields))) {
                    Record dData = new Record(schemas[le].getSchema());
                    for (int i = 0; i < fields.length; i++) {
                        dData.put(fields[i], upper.values[i]);
                    }
                    tree[le - 1].deleteForward(upper, new KeyofBTree(data, keyFields[le]));
                    tree[le - 1].insertForward(new KeyofBTree(data, fields), new KeyofBTree(data, keyFields[le]));
                    updateDelete(dData, le);
                    updateInsert(data, le);
                    return;
                }
            }
        }
        cachOperate(data, (byte) 3, le);
    }

    private void updateDelete(Record data, int le) throws IOException {
        cachOperate(data, (byte) 4, le);
    }

    private List<Record> getAllNested(Record data, int le) {
        assert (le < layer);
        return tree[le].findForward(new KeyofBTree(data, keyFields[le]));
    }

    private void updateInsert(Record data, int le) throws IOException {
        extraCachOperate(data, (byte) 5, le);
    }

    private void deleteToCach(Record data, int le) throws IOException {
        KeyofBTree key = new KeyofBTree(data, keyFields[le]);
        if (le < (layer - 1)) {
            List<Record> nest = tree[le].findForward(key);
            nestDelete(nest, le + 1, le);
            tree[le].deleteForward(key);
        }
        if (le > 0) {
            KeyofBTree upper = findForeignKey(key, le);
            tree[le - 1].deleteForward(upper, key);
        }
        cach.add(le, key, new FlagData((byte) 2, null, le));
    }

    private void nestDelete(List<Record> nest, int le, int ne) {
        for (Record r : nest) {
            KeyofBTree key = new KeyofBTree(r);
            if (le < (layer - 1)) {
                List<Record> nn = tree[le].findForward(key);
                nestDelete(nn, le + 1, ne);
                tree[le].deleteForward(key);
            }
            FlagData fd = cach.find(key, le);
            if (fd != null && fd.getFlag() == (byte) 4)
                cach.extraDelete(key, le);
            cach.add(le, key, new FlagData((byte) 2, null, ne));
        }
    }

    private void cachOperate(Record data, byte flag, int le) {
        cach.add(data, flag, le);
    }

    private void extraCachOperate(Record data, byte flag, int le) {
        cach.extraAdd(data, flag, le);
    }

    public void load(NestSchema schema) throws IOException {
        File file = schema.getPrFile();
        int[] keyFields = schema.getKeyFields();
        Schema s = schema.getSchema();
        long start = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        InsertAvroColumnWriter<ComparableKey, Record> writer =
                new InsertAvroColumnWriter<ComparableKey, Record>(s, resultPath, keyFields, free, mul, blockSize);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            Record record = arrToRecord(tmp, s);
            writer.append(new ComparableKey(record, keyFields), record);
        }
        reader.close();
        reader = null;
        int index = writer.flush();
        File[] files = new File[index];
        for (int i = 0; i < index; i++) {
            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".trv");
        }
        if (index == 1) {
            merge(files);
            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
            new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
        } else {
            merge(files);
            writer.mergeFiles(files, tmpPath);
        }

        deleteFile(schema.getPrFile().getPath());
        long end = System.currentTimeMillis();
        System.out.println(schema.getSchema().getName() + "\tsort trevni time: " + (end - start) + "ms");
    }

    public void createReader() throws IOException {
        reader = new ColumnReader<Record>(new File(resultPath + "result.trv"));
    }

    public void dLoad(NestSchema schema1, NestSchema schema2) throws IOException {
        int[] fields1 = keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields());
        int numElements1 = toSortAvroFile(schema1, fields1);//write file1 according to (outkey+pkey) order
        int numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());//write file2 according to pkey order
        tree[0].create((int) (numElements2 / 500));
        tree[1].create((int) (numElements1 / 500));

        BloomFilterBuilder builder1 = createBloom(numElements1, 1);
        BloomFilterBuilder builder2 = createBloom(numElements2, 0);
        long start = System.currentTimeMillis();
        SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeSchema(), fields1);
        SortedAvroReader reader2 =
                new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
        InsertAvroColumnWriter<ComparableKey, Record> writer = new InsertAvroColumnWriter<ComparableKey, Record>(
                schema2.getNestedSchema(), resultPath, schema2.getKeyFields(), free, mul, blockSize);

        Record record1 = reader1.next();
        builder1.add(record1);
        while (reader2.hasNext()) {
            Record record2 = reader2.next();
            builder2.add(record2);
            ComparableKey k2 = new ComparableKey(record2, schema2.getKeyFields());
            List<Record> arr = new ArrayList<Record>();
            while (true) {
                ComparableKey k1 = new ComparableKey(record1, schema1.getOutKeyFields());
                if (k2.compareTo(k1) == 0) {
                    arr.add(record1);
                    if (reader1.hasNext()) {
                        record1 = reader1.next();
                        builder1.add(record1);
                        continue;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
            writer.append(k2, record);
        }
        builder1.write();
        builder2.write();
        reader1.close();
        reader2.close();
        reader1 = null;
        reader2 = null;
        int index = writer.flush();
        File[] files = new File[index];
        for (int i = 0; i < index; i++) {
            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".trv");
        }
        if (index == 1) {
            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
            new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
            reader = new ColumnReader<Record>(new File(resultPath + "result.trv"));
            reader.createSchema(nestKeySchemas[0]);
            Integer[] in = new Integer[layer];
            for (int m = 0; m < layer; m++)
                in[m] = 0;
            while (reader.hasNext()) {
                Record record = reader.next();
                updateTree(0, null, in, record);
            }
            for (int i = 0; i < layer; i++)
                tree[i].write();
        } else {
            merge(files);
            writer.mergeFiles(files, tmpPath);
        }

        deleteFile(schema1.getPath());
        deleteFile(schema2.getPath());
        long end = System.currentTimeMillis();
        System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort trevni time: "
                + (end - start) + "ms");
    }

    public Record join(Schema schema, Record record, List<Record> arr) {
        Record result = new Record(schema);
        List<Field> fs = schema.getFields();
        for (int i = 0; i < fs.size() - 1; i++) {
            result.put(i, record.get(i));
        }
        result.put(fs.size() - 1, arr);
        return result;
    }

    public void prLoad(NestSchema schema1, NestSchema schema2) throws IOException {
        int[] fields1 = keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields());
        int numElements1 = toSortAvroFile(schema1, fields1);//write file1 according to (outkey+pkey) order
        int numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());//write file2 according to pkey order
        tree[layer - 1].create((int) (numElements1 / 500));
        tree[layer - 2].create((int) (numElements2 / 500));

        BloomFilterBuilder builder1 = createBloom(numElements1, (layer - 1));
        BloomFilterBuilder builder2 = createBloom(numElements2, (layer - 2));

        long start = System.currentTimeMillis();
        SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeSchema(), fields1);
        SortedAvroReader reader2 =
                new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
        SortedAvroWriter<ComparableKey, Record> writer =
                new SortedAvroWriter<ComparableKey, Record>(tmpPath, schema2.getEncodeNestedSchema(), free, mul);
        int[] sortFields = keyJoin(schema2.getOutKeyFields(), schema2.getKeyFields());

        Record record1 = reader1.next();
        builder1.add(record1);
        while (reader2.hasNext()) {
            Record record2 = reader2.next();
            builder2.add(record2);
            ComparableKey k2 = new ComparableKey(record2, schema2.getKeyFields());
            List<Record> arr = new ArrayList<Record>();
            List<Record> v = new ArrayList<Record>();
            while (true) {
                ComparableKey k1 = new ComparableKey(record1, schema1.getOutKeyFields());
                if (k2.compareTo(k1) == 0) {
                    arr.add(record1);
                    Record aa = new Record(keySchemas[layer - 1]);
                    setKey(aa, record1, keyFields[layer - 1]);
                    v.add(aa);
                    if (reader1.hasNext()) {
                        record1 = reader1.next();
                        builder1.add(record1);
                        continue;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
            Record k = new Record(keySchemas[layer - 2]);
            setKey(k, record2, keyFields[layer - 2]);
            writer.append(new ComparableKey(record, sortFields), record);
        }
        builder1.write();
        builder2.write();
        reader1.close();
        reader2.close();
        reader1 = null;
        reader2 = null;
        writer.flush();
        deleteFile(schema1.getPath());
        deleteFile(schema2.getPath());
        moveTo(tmpPath, schema2.getPath());
        long end = System.currentTimeMillis();
        System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort avro time: "
                + (end - start) + "ms");
    }

    public void orLoad(NestSchema schema1, NestSchema schema2, int index) throws IOException {
        int numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());//write file2 according to pkey order
        BloomFilterBuilder builder2 = createBloom(numElements2, (index - 1));
        tree[index - 1].create((int) (numElements2 / 500));

        //Sort file1 and file2 based on the foreign key of file1
        long start = System.currentTimeMillis();
        SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeNestedSchema(),
                keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields()));
        SortedAvroReader reader2 =
                new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
        SortedAvroWriter<ComparableKey, Record> writer =
                new SortedAvroWriter<ComparableKey, Record>(tmpPath, schema2.getEncodeNestedSchema(), free, mul);
        int[] sortFields = keyJoin(schema2.getOutKeyFields(), schema2.getKeyFields());

        Record record1 = reader1.next();
        while (reader2.hasNext()) {
            Record record2 = reader2.next();
            builder2.add(record2);
            ComparableKey k2 = new ComparableKey(record2, schema2.getKeyFields());
            List<Record> arr = new ArrayList<Record>();
            List<Record> v = new ArrayList<Record>();
            while (true) {
                ComparableKey k1 = new ComparableKey(record1, schema1.getOutKeyFields());
                if (k2.compareTo(k1) == 0) {
                    arr.add(record1);
                    Record aa = new Record(keySchemas[index]);
                    setKey(aa, record1, keyFields[index]);
                    v.add(aa);
                    if (reader1.hasNext()) {
                        record1 = reader1.next();
                        continue;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
            Record k = new Record(keySchemas[index - 1]);
            setKey(k, record2, keyFields[index - 1]);
            writer.append(new ComparableKey(record, sortFields), record);
        }
        builder2.write();
        reader1.close();
        reader2.close();
        reader1 = null;
        reader2 = null;
        writer.flush();
        deleteFile(schema1.getPath());
        deleteFile(schema2.getPath());
        moveTo(tmpPath, schema2.getPath());
        long end = System.currentTimeMillis();
        System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort avro time: "
                + (end - start) + "ms");
    }

    public void laLoad(NestSchema schema1, NestSchema schema2) throws IOException {
        int numElements2 = toSortAvroFile(schema2, schema2.getKeyFields());//write file2 according to pkey order
        BloomFilterBuilder builder2 = createBloom(numElements2, 0);
        tree[0].create((int) (numElements2 / 500));

        long start = System.currentTimeMillis();
        SortedAvroReader reader1 = new SortedAvroReader(schema1.getPath(), schema1.getEncodeNestedSchema(),
                keyJoin(schema1.getOutKeyFields(), schema1.getKeyFields()));
        SortedAvroReader reader2 =
                new SortedAvroReader(schema2.getPath(), schema2.getEncodeSchema(), schema2.getKeyFields());
        InsertAvroColumnWriter<ComparableKey, Record> writer = new InsertAvroColumnWriter<ComparableKey, Record>(
                schema2.getNestedSchema(), resultPath, schema2.getKeyFields(), free, mul, blockSize);

        Record record1 = reader1.next();
        while (reader2.hasNext()) {
            Record record2 = reader2.next();
            builder2.add(record2);
            ComparableKey k2 = new ComparableKey(record2, schema2.getKeyFields());
            List<Record> arr = new ArrayList<Record>();
            List<Record> v = new ArrayList<Record>();
            while (true) {
                ComparableKey k1 = new ComparableKey(record1, schema1.getOutKeyFields());
                if (k2.compareTo(k1) == 0) {
                    arr.add(record1);
                    Record aa = new Record(keySchemas[1]);
                    setKey(aa, record1, keyFields[1]);
                    v.add(aa);
                    if (reader1.hasNext()) {
                        record1 = reader1.next();
                        continue;
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            Record record = join(schema2.getEncodeNestedSchema(), record2, arr);
            Record k = new Record(keySchemas[0]);
            setKey(k, record2, keyFields[0]);
            writer.append(k2, record);
        }
        builder2.write();
        reader1.close();
        reader2.close();
        reader1 = null;
        reader2 = null;
        int index = writer.flush();
        File[] files = new File[index];
        for (int i = 0; i < index; i++) {
            files[i] = new File(resultPath + "file" + String.valueOf(i) + ".trv");
        }
        if (index == 1) {
            new File(resultPath + "file0.head").renameTo(new File(resultPath + "result.head"));
            new File(resultPath + "file0.trv").renameTo(new File(resultPath + "result.trv"));
            reader = new ColumnReader<Record>(new File(resultPath + "result.trv"));
            reader.createSchema(nestKeySchemas[0]);
            Integer[] in = new Integer[layer];
            for (int m = 0; m < layer; m++)
                in[m] = 0;
            while (reader.hasNext()) {
                Record record = reader.next();
                updateTree(0, null, in, record);
            }
            for (int i = 0; i < layer; i++) {
                tree[i].write();
            }
        } else {
            merge(files);
            writer.mergeFiles(files, tmpPath);
        }

        deleteFile(schema1.getPath());
        deleteFile(schema2.getPath());
        long end = System.currentTimeMillis();
        System.out.println(schema2.getSchema().getName() + "+" + schema1.getSchema().getName() + "\tsort trevni time: "
                + (end - start) + "ms");
    }

    public void merge(File[] files) throws IOException {
        long t1 = System.currentTimeMillis();
        Integer[] index = new Integer[layer];
        for (int i = 0; i < layer; i++)
            index[i] = 0;
        int[] la = new int[layer - 1];
        for (int i = 0; i < la.length; i++) {
            la[i] = schemas[i].getKeyFields().length;
        }
        SortTrevniReader re = new SortTrevniReader(files, nestKeySchemas[0], tmpPath);
        while (re.hasNext()) {
            Record record = re.next().getRecord();
            updateTree(0, null, index, record);
        }
        for (int i = 0; i < layer; i++) {
            tree[i].write();
        }
        re.close();
        re = null;
        long t2 = System.currentTimeMillis();
        System.out.println("$$$merge read + btree time" + (t2 - t1));
        //        return re.getGap();
    }

    public void deleteFile(String path) throws IOException {
        File file = new File(path);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                deleteFile(files[i].getPath());
            }
        } else {
            shDelete(path);
        }
    }

    public static String writeKeyRecord(Record record) {
        StringBuilder str = new StringBuilder();
        str.append("{");
        Schema s = record.getSchema();
        List<Field> fs = s.getFields();
        int len = fs.size();
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                if (isSimple(fs.get(i))) {
                    str.append(record.get(i));
                } else {
                    if (fs.get(i).schema().getType() == Type.ARRAY) {
                        List<Record> rs = (List<Record>) record.get(i);
                        if (rs != null) {
                            int l = rs.size();
                            if (l > 0) {
                                str.append(writeKeyRecord(rs.get(0)));
                                for (int j = 1; j < l; j++) {
                                    str.append(",");
                                    str.append(writeKeyRecord(rs.get(j)));
                                }
                            }
                        }
                    }
                }
                if (i < len - 1) {
                    str.append("|");
                }
            }
        }
        str.append("}");
        return str.toString();
    }

    public static Record readKeyRecord(Schema s, String str) {
        char[] ss = str.toCharArray();
        assert (ss[0] == '{' && ss[ss.length - 1] == '}');
        int index = 1;
        Record r = new Record(s);
        int i = 0;
        for (Field f : s.getFields()) {
            if (isSimple(f)) {
                StringBuilder v = new StringBuilder();
                while (index < (ss.length - 1) && ss[index] != '|') {
                    v.append(ss[index]);
                    index++;
                }
                r.put(i++, getValue(f, v.toString()));
            } else {
                List<Record> record = new ArrayList<Record>();
                while (index < ss.length - 1) {
                    assert (ss[index] == '{');
                    index++;
                    int tt = 1;
                    StringBuilder xx = new StringBuilder();
                    xx.append('{');
                    while (tt > 0) {
                        if (ss[index] == '{') {
                            tt++;
                        }
                        if (ss[index] == '}') {
                            tt--;
                        }
                        xx.append(ss[index]);
                        index++;
                    }
                    record.add(readKeyRecord(f.schema().getElementType(), xx.toString()));
                    if (ss[index] == ',') {
                        index++;
                    } else {
                        break;
                    }
                }
                r.put(i++, record);
            }
            if (i < s.getFields().size()) {
                assert (ss[index] == '|');
                index++;
            }
        }
        assert (index == ss.length - 1);
        return r;
    }

    public static boolean isSimple(Field f) {
        switch (f.schema().getType()) {
            case INT:
            case LONG:
            case STRING:
            case BYTES:
                return true;
            case ARRAY:
                return false;
        }
        throw new ClassCastException("cannot support the key type:" + f.schema().getType());
    }

    public static Object getValue(Field f, String s) {
        if (s.equals(""))
            return null;
        switch (f.schema().getType()) {
            case INT:
                return Integer.parseInt(s);
            case LONG:
                return Long.parseLong(s);
            case STRING:
            case BYTES:
                return s;
        }
        throw new ClassCastException("cannot support the key type:" + f.schema().getType());
    }

    public void moveTo(String path, String toPath) {
        File file = new File(path);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                files[i].renameTo(new File(toPath + files[i].getName()));
            }
        }
    }

    public int toSortAvroFile(NestSchema schema, int[] keyFields) throws IOException {
        int numElements = 0;
        long start = System.currentTimeMillis();
        File file = schema.getPrFile();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        SortedAvroWriter<ComparableKey, Record> writer =
                new SortedAvroWriter<ComparableKey, Record>(schema.getPath(), schema.getEncodeSchema(), free, mul);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|");
            numElements++;
            Record record = arrToRecord(tmp, schema.getEncodeSchema());
            writer.append(new ComparableKey(record, keyFields), record);
        }
        reader.close();
        writer.flush();
        long end = System.currentTimeMillis();
        System.out.println(schema.getSchema().getName() + "\tsort avro time: " + (end - start) + "ms");
        return numElements;
    }

    public int[] keyJoin(int[] key1, int[] key2) {
        int len1 = key1.length;
        int len2 = key2.length;
        int[] result = new int[(len1 + len2)];
        for (int i = 0; i < len1; i++) {
            result[i] = key1[i];
        }
        for (int i = 0; i < len2; i++) {
            result[(i + len1)] = key2[i];
        }
        return result;
    }

    public Record arrToRecord(String[] arr, Schema s) {
        Record record = new Record(s);
        List<Field> fs = s.getFields();
        for (int i = 0; i < arr.length; i++) {
            switch (fs.get(i).schema().getType()) {
                case STRING: {
                    record.put(i, arr[i]);
                    break;
                }
                case BYTES: {
                    record.put(i, ByteBuffer.wrap(arr[i].getBytes()));
                    break;
                }
                case INT: {
                    record.put(i, Integer.parseInt(arr[i]));
                    break;
                }
                case LONG: {
                    record.put(i, Long.parseLong(arr[i]));
                    break;
                }
                case FLOAT: {
                    record.put(i, Float.parseFloat(arr[i]));
                    break;
                }
                case DOUBLE: {
                    record.put(i, Double.parseDouble(arr[i]));
                    break;
                }
                case BOOLEAN: {
                    record.put(i, Boolean.getBoolean(arr[i]));
                    break;
                }
                default: {
                    throw new ClassCastException("This type " + fs.get(i).schema().getType() + " is not supported!");
                }
            }
        }
        return record;
    }

    public long getNumLines(File file) throws IOException {
        long len = 0;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        while (reader.readLine() != null) {
            len++;
        }
        reader.close();
        return len;
    }
}

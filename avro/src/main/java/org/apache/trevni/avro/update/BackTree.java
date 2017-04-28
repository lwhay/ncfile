package org.apache.trevni.avro.update;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;

import btree.BtreeCluster;

public class BackTree {
    private int[] keyFields;
    private int[] valueFields;
    private int[] vOFKFields;
    private Schema schema;
    private Schema valueSchema;
    private String path;
    private BtreeCluster<KeyofBTree, KeyofBTree> btree;
    private List<Entry<KeyofBTree, KeyofBTree>> cache;
    private boolean isBtree;
    private int index;
    private int fIn;

    private static final int MAX = 500000;
    final int blocksize = 512;
    private int cachesize = 3000;
    //    final int datacachesize = 1000;
    final int nodenumofblock = 1;

    public BackTree(int[] keyFields, int[] valueFields, Schema schema, String path) {
        this.keyFields = keyFields;
        this.valueFields = valueFields;
        this.schema = schema;
        List<Field> fields = new ArrayList<Field>();
        List<Field> fs = schema.getFields();
        for (int i = 0; i < valueFields.length; i++) {
            Field f = fs.get(valueFields[i]);
            Type type = f.schema().getType();
            if (type.compareTo(Type.ARRAY) == 0 | type.compareTo(Type.ENUM) == 0 | type.compareTo(Type.FIXED) == 0
                    | type.compareTo(Type.MAP) == 0 | type.compareTo(Type.NULL) == 0) {
                throw new ClassCastException("This type is not supported: " + type);
            }
            fields.add(new Schema.Field(f.name(), f.schema(), null, null));
        }
        valueSchema = Schema.createRecord(schema.getName(), null, null, false, fields);
        isBtree = isBtree(keyFields, valueFields);
        if (isBtree) {
            assert (path != null);
            this.path = path;
            //            create();
        }
        index = 0;
    }

    public void create() {
        create(cachesize);
    }

    public void close() {
        if (isBtree && btree != null) {
            btree.close();
        }
    }

    private boolean isBtree(int[] key, int[] value) {
        if (value == null) {
            return true;
        }
        int[] fields = new int[value.length];
        for (int i = 0; i < value.length; i++) {
            int j = 0;
            while (j < key.length) {
                if (key[j] == value[i]) {
                    fields[i] = j;
                    break;
                } else {
                    j++;
                }
            }
            if (j == key.length) {
                return true;
            }
        }
        vOFKFields = fields;
        return false;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public boolean isbtree() {
        return isBtree;
    }

    public void create(int cacheSize) {
        if (isBtree) {
            File p = new File(path + "/file.db");
            if (p.exists()) {
                btree = new BtreeCluster<KeyofBTree, KeyofBTree>();
                btree.open(p.getPath());
                cachesize = btree.getCacheSize();
            } else {
                if (!p.getParentFile().exists()) {
                    p.getParentFile().mkdirs();
                }
                cachesize = cacheSize;
                fIn = 0;
                cache = new ArrayList<Entry<KeyofBTree, KeyofBTree>>();
            }
        }
    }

    public void createMerge(int cacheSize) {
        if (isBtree) {
            //            File p = new File(path + "/file.db");
            close();
            btree = null;
            //            p.delete();
            //            new File(path + "/file.db.meta.data").delete();
            NestManager.shDelete(path + "/file.db");
            NestManager.shDelete(path + "/file.db.meta.data");
            fIn = 0;
            cache = new ArrayList<Entry<KeyofBTree, KeyofBTree>>();
            cachesize = cacheSize;
            //            btree = new BtreeCluster<KeyofBTree, KeyofBTree>(nodenumofblock, p.getPath(), cachesize, blocksize,
            //                    0.6f);
        }
    }

    public void write() {
        if (isBtree) {
            if (fIn == 0)
                commit();
            merge();
        }
    }

    public void commit() {
        if (isBtree) {
            if (index != 0) {
                btree = new BtreeCluster<KeyofBTree, KeyofBTree>(nodenumofblock, path + "/file" + fIn + ".db",
                        (int) (index / 500), blocksize, 0.6f);
                fIn++;
                Collections.sort(cache, new EntryComparator<KeyofBTree, KeyofBTree>());
                for (Entry<KeyofBTree, KeyofBTree> en : cache) {
                    btree.insert(en.getKey(), en.getValue());
                }
                cache.clear();
                btree.close();
                btree = null;
                index = 0;
            }
        }
    }

    public void merge() {
        if (isBtree) {
            if (index == 0)
                mergeFiles();
            else
                mergeFilesAndCache();
        }
    }

    public void mergeFilesAndCache() {
        if (isBtree) {
            btree = new BtreeCluster<KeyofBTree, KeyofBTree>(nodenumofblock, path + "/file.db", cachesize, blocksize,
                    0.6f);
            Collections.sort(cache, new EntryComparator<KeyofBTree, KeyofBTree>());
            Iterator<Entry<KeyofBTree, KeyofBTree>> it = cache.iterator();
            Entry<KeyofBTree, KeyofBTree> a1 = it.next();
            File[] files = new File[fIn];
            for (int i = 0; i < fIn; i++) {
                files[i] = new File(path + "/file" + i + ".db");
            }
            SortedBTreeReader reader = new SortedBTreeReader(files);
            while (reader.hasNext()) {
                Entry<KeyofBTree, KeyofBTree> a2 = reader.next();
                while (a1 != null && a1.getKey().compareTo(a2.getKey()) < 0) {
                    btree.insert(a1.getKey(), a1.getValue());
                    if (it.hasNext())
                        a1 = it.next();
                    else
                        a1 = null;
                }
                btree.insert(a2.getKey(), a2.getValue());
            }
            reader.close();
            reader = null;
            while (a1 != null) {
                btree.insert(a1.getKey(), a1.getValue());
                if (it.hasNext())
                    a1 = it.next();
                else
                    a1 = null;
            }
            cache.clear();
            btree.close();
            btree = null;
            for (File file : files) {
                //                file.delete();
                //                new File(file.getPath() + ".meta.data").delete();
                NestManager.shDelete(file.getAbsolutePath());
                NestManager.shDelete(file.getAbsolutePath() + ".meta.data");
            }
        }
    }

    public void mergeFiles() {
        if (isBtree) {
            if (fIn == 1) {
                new File(path + "/file0.db").renameTo(new File(path + "/file.db"));
                new File(path + "/file0.db.meta.data").renameTo(new File(path + "/file.db.meta.data"));
            } else {
                btree = new BtreeCluster<KeyofBTree, KeyofBTree>(nodenumofblock, path + "/file.db", cachesize,
                        blocksize, 0.6f);
                File[] files = new File[fIn];
                for (int i = 0; i < fIn; i++) {
                    files[i] = new File(path + "/file" + i + ".db");
                }
                SortedBTreeReader reader = new SortedBTreeReader(files);
                while (reader.hasNext()) {
                    Entry<KeyofBTree, KeyofBTree> en = reader.next();
                    btree.insert(en.getKey(), en.getValue());
                }
                reader.close();
                btree.close();
                reader = null;
                btree = null;
                for (File file : files) {
                    //                    file.delete();
                    //                    new File(file.getPath() + ".meta.data").delete();
                    NestManager.shDelete(file.getAbsolutePath());
                    NestManager.shDelete(file.getAbsolutePath() + ".meta.data");
                }
            }
        }
    }

    public void put(Record record) {
        put(new CombKey(record, keyFields), new CombKey(record, valueFields));
    }

    public void put(Record key, Record value) {
        put(new CombKey(key, keyFields.length), new CombKey(value, valueFields.length));
    }

    public void put(CombKey key, CombKey value) {
        if (isBtree) {
            cache.add(new MyEntry<KeyofBTree, KeyofBTree>(new KeyofBTree(key), new KeyofBTree(value)));
            index++;
            if (index >= MAX) {
                commit();
            }
        }
    }

    public CombKey get(Record key, boolean isKey) {
        return isKey ? get(new CombKey(key)) : get(new CombKey(key, keyFields));
    }

    public CombKey get(CombKey key) {
        if (isBtree) {
            KeyofBTree rr = btree.find(new KeyofBTree(key));
            if (rr == null)
                return null;
            return new CombKey(rr.values, rr.types);
        } else {
            return key.get(vOFKFields);
        }
    }

    public Record getRecord(Record key, boolean isKey) {
        return isKey ? getRecord(new CombKey(key)) : getRecord(new CombKey(key, keyFields));
    }

    public Record getRecord(CombKey key) {
        Record res = new Record(valueSchema);
        if (isBtree) {
            KeyofBTree rr = btree.find(new KeyofBTree(key));
            if (rr == null)
                return null;
            for (int i = 0; i < valueFields.length; i++) {
                if (rr.types[i]) {
                    res.put(i, Integer.parseInt(rr.values[i].toString()));
                } else {
                    res.put(i, Long.parseLong(rr.values[i].toString()));
                }
            }
        } else {
            Object[] rr = key.get();
            for (int i = 0; i < valueFields.length; i++) {
                if (key.getType(i)) {
                    res.put(i, Integer.parseInt(rr[vOFKFields[i]].toString()));
                } else {
                    res.put(i, Long.parseLong(rr[vOFKFields[i]].toString()));
                }
            }
        }
        return res;
    }

    public class SortedBTreeReader implements Closeable {
        private Iterator<BtreeCluster.Entry<KeyofBTree, KeyofBTree>>[] btreeIter;
        private BtreeCluster<KeyofBTree, KeyofBTree>[] btree;
        private Entry<KeyofBTree, KeyofBTree>[] values;
        private int[] sort;
        private int start;

        public SortedBTreeReader(File[] files) {
            btree = new BtreeCluster[files.length];
            btreeIter = new Iterator[files.length];
            int i = 0;
            for (File file : files) {
                btree[i] = new BtreeCluster<KeyofBTree, KeyofBTree>();
                //                BtreeCluster<KeyofBTree, KeyofBTree> tree = new BtreeCluster<KeyofBTree, KeyofBTree>();
                btree[i].open(file.getPath(), 10);
                btreeIter[i] = btree[i].iterator();
                i++;
            }
            values = new Entry[files.length];
            for (i = 0; i < values.length; i++) {
                BtreeCluster.Entry<KeyofBTree, KeyofBTree> v = btreeIter[i].next();
                values[i] = new MyEntry<KeyofBTree, KeyofBTree>(v.getKey(), v.getValue());
            }
            sort = new int[files.length];
            for (i = 0; i < sort.length; i++) {
                sort[i] = i;
            }
            for (i = 0; i < (sort.length - 1); i++) {
                for (int j = (i + 1); j < sort.length; j++) {
                    if (values[sort[i]].getKey().compareTo(values[sort[j]].getKey()) > 0) {
                        int tmp = sort[i];
                        sort[i] = sort[j];
                        sort[j] = tmp;
                    }
                }
            }
            start = 0;
        }

        public Entry<KeyofBTree, KeyofBTree> next() {
            Entry<KeyofBTree, KeyofBTree> res = values[sort[start]];
            if (!btreeIter[sort[start]].hasNext()) {
                start++;
            } else {
                BtreeCluster.Entry<KeyofBTree, KeyofBTree> v = btreeIter[sort[start]].next();
                values[sort[start]] = new MyEntry<KeyofBTree, KeyofBTree>(v.getKey(), v.getValue());
                int m = start;
                for (int i = (start + 1); i < sort.length; i++) {
                    if (values[sort[start]].getKey().compareTo(values[sort[i]].getKey()) > 0)
                        m++;
                    else
                        break;
                }
                if (m > start) {
                    int tmp = sort[start];
                    for (int i = start; i < m; i++) {
                        sort[i] = sort[i + 1];
                    }
                    sort[m] = tmp;
                }
            }
            return res;
        }

        public boolean hasNext() {
            return start < sort.length;
        }

        public void close() {
            for (int i = 0; i < btree.length; i++) {
                btree[i].close();
                btree[i] = null;
                btreeIter[i] = null;
            }
        }
    }
}

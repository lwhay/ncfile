package org.apache.trevni.avro.update;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import btree.BtreeCluster;

public class OffsetTree {
    private int[] keyFields;
    private Schema schema;
    private String path;
    private BtreeCluster<KeyofBTree, Integer> btree;
    private List<Entry<KeyofBTree, Integer>> cache;
    private int index;
    private int fIn;

    private static final int MAX = 1000000;
    final int blocksize = 256;
    private int cachesize = 3000;
    //    final int datacachesize = 100000;
    final int nodenumofblock = 1;

    public OffsetTree(int[] keyFields, Schema schema, String path) {
        this.keyFields = keyFields;
        this.schema = schema;
        this.path = path;
        //        create();
        index = 0;
    }

    public void create() {
        create(cachesize);
    }

    public void close() {
        if (btree != null)
            btree.close();
    }

    public void create(int cacheSize) {
        File p = new File(path + "/file.db");
        if (p.exists()) {
            btree = new BtreeCluster<KeyofBTree, Integer>();
            btree.open(path + "/file.db");
            cachesize = btree.getCacheSize();
        } else {
            if (!p.getParentFile().exists()) {
                p.getParentFile().mkdirs();
            }
            cachesize = cacheSize;
            fIn = 0;
            cache = new ArrayList<Entry<KeyofBTree, Integer>>();
            //            btree = new Btree<KeyofBTree, Long>(nodenumofblock, path + "file" + fIn + ".db", cachesize, blocksize,
            //                    0.6f);
        }
    }

    public void createMerge(int cacheSize) throws IOException {
        //        File p = new File(path + "/file.db");
        close();
        btree = null;
        //        p.delete();
        //        new File(path + "/file.db.meta.data").delete();
        NestManager.shDelete(path + "/file.db");
        NestManager.shDelete(path + "/file.db.meta.data");
        fIn = 0;
        cache = new ArrayList<Entry<KeyofBTree, Integer>>();
        cachesize = cacheSize;
    }

    public void write() {
        if (fIn == 0)
            commit();
        merge();
    }

    public void put(Record data, int value, boolean isKey) {
        //        KeyofBTree kb = isKey ? new KeyofBTree(new CombKey(key)) : new KeyofBTree(new CombKey(key, keyFields));
        //        btree.insert(kb, value);
        CombKey key = isKey ? new CombKey(data, keyFields.length) : new CombKey(data, keyFields);
        put(key, value);
    }

    public void put(CombKey key, int value) {
        cache.add(new MyEntry<KeyofBTree, Integer>(new KeyofBTree(key), value));
        index++;
        if (index >= MAX) {
            commit();
        }
    }

    public void commit() {
        if (index != 0) {
            btree = new BtreeCluster<KeyofBTree, Integer>(nodenumofblock, path + "/file" + fIn + ".db",
                    (int) (index / 500), blocksize, 0.6f);
            fIn++;
            Collections.sort(cache, new EntryComparator<KeyofBTree, Integer>());
            for (Entry<KeyofBTree, Integer> en : cache) {
                btree.insert(en.getKey(), en.getValue());
            }
            cache.clear();
            btree.close();
            btree = null;
            index = 0;
        }
    }

    public void merge() {
        if (index == 0)
            mergeFiles();
        else
            mergeFilesAndCache();
    }

    public void mergeFilesAndCache() {
        btree = new BtreeCluster<KeyofBTree, Integer>(nodenumofblock, path + "/file.db", cachesize, blocksize, 0.6f);
        Collections.sort(cache, new EntryComparator<KeyofBTree, Integer>());
        Iterator<Entry<KeyofBTree, Integer>> it = cache.iterator();
        Entry<KeyofBTree, Integer> a1 = it.next();
        File[] files = new File[fIn];
        for (int i = 0; i < fIn; i++) {
            files[i] = new File(path + "/file" + i + ".db");
        }
        SortedBTreeReader reader = new SortedBTreeReader(files);
        while (reader.hasNext()) {
            Entry<KeyofBTree, Integer> a2 = reader.next();
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
        close();
        btree = null;
        for (File file : files) {
            //            file.delete();
            //            new File(file.getPath() + ".meta.data").delete();
            NestManager.shDelete(file.getAbsolutePath());
            NestManager.shDelete(file.getAbsolutePath() + ".meta.data");
        }
        create();
    }

    public void mergeFiles() {
        if (fIn == 1) {
            new File(path + "/file0.db").renameTo(new File(path + "/file.db"));
            new File(path + "/file0.db.meta.data").renameTo(new File(path + "/file.db.meta.data"));
        } else {
            btree = new BtreeCluster<KeyofBTree, Integer>(nodenumofblock, path + "/file.db", cachesize, blocksize,
                    0.6f);
            File[] files = new File[fIn];
            for (int i = 0; i < fIn; i++) {
                files[i] = new File(path + "/file" + i + ".db");
            }
            SortedBTreeReader reader = new SortedBTreeReader(files);
            while (reader.hasNext()) {
                Entry<KeyofBTree, Integer> en = reader.next();
                btree.insert(en.getKey(), en.getValue());
            }
            reader.close();
            btree.close();
            reader = null;
            btree = null;
            for (File file : files) {
                //                file.delete();
                //                new File(file.getPath() + ".meta.data").delete();
                NestManager.shDelete(file.getAbsolutePath());
                NestManager.shDelete(file.getAbsolutePath() + ".meta.data");
            }
        }
    }

    public Object get(Record key, boolean isKey) {
        return isKey ? btree.find(new KeyofBTree(new CombKey(key)))
                : btree.find(new KeyofBTree(new CombKey(key, keyFields)));
    }

    public Object get(Record key, int[] fields) {
        return btree.find(new KeyofBTree(new CombKey(key, fields)));
    }

    public Integer get(CombKey key) {
        return btree.find(new KeyofBTree(key));
    }

    public class SortedBTreeReader {
        private Iterator<BtreeCluster.Entry<KeyofBTree, Integer>>[] btreeIter;
        private BtreeCluster<KeyofBTree, Integer>[] btree;
        private Entry<KeyofBTree, Integer>[] values;
        private int[] sort;
        private int start;

        public SortedBTreeReader(File[] files) {
            btreeIter = new Iterator[files.length];
            btree = new BtreeCluster[files.length];
            int i = 0;
            for (File file : files) {
                btree[i] = new BtreeCluster<KeyofBTree, Integer>();
                btree[i].open(file.getPath(), 10);
                btreeIter[i] = btree[i].iterator();
                i++;
            }
            values = new Entry[files.length];
            for (i = 0; i < values.length; i++) {
                BtreeCluster.Entry<KeyofBTree, Integer> v = btreeIter[i].next();
                values[i] = new MyEntry<KeyofBTree, Integer>(v.getKey(), v.getValue());
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

        public Entry<KeyofBTree, Integer> next() {
            Entry<KeyofBTree, Integer> res = values[sort[start]];
            if (!btreeIter[sort[start]].hasNext()) {
                start++;
            } else {
                BtreeCluster.Entry<KeyofBTree, Integer> v = btreeIter[sort[start]].next();
                values[sort[start]] = new MyEntry<KeyofBTree, Integer>(v.getKey(), v.getValue());
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

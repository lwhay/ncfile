package neci.ncfile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import btree.Btree;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

public class LayerTree {
    private int[] keyFields;
    private String path;
    //    private Schema schema;
    private boolean isHighest;

    private boolean hasForwardTree;
    private int[] forwardValueFields;
    private Schema forwardValueSchema;

    boolean hasBackTree;
    //    private int[] backValueFields;
    private int[] backValueofKeyFields;
    //    private Schema backValueSchema;

    final int blocksize = 4096;
    private int cachesize = 3000;
    final int datacachesize = 100;
    final int nodenumofblock = 1;

    private Btree<KeyofBTree, String> btree;
    private List<Entry<KeyofBTree, String>> cache;
    private int fIn;
    private int index;
    private static final int MAX = 500000;
    private HashMap<KeyofBTree, List<FlagKey>> forwardCache;

    public LayerTree(String path, int[] keyFields, boolean isHighest, boolean isLowest, int[] forwardValueFields,
            Schema forwardValueSchema, int[] backValueFields) {
        this.path = path;
        this.keyFields = keyFields;
        //        this.schema = schema;
        this.isHighest = isHighest;
        hasForwardTree = !isLowest;
        if (hasForwardTree) {
            this.forwardValueFields = forwardValueFields;
            this.forwardValueSchema = forwardValueSchema;
        }

        if (!isHighest) {
            //            List<Field> fields = new ArrayList<Field>();
            //            List<Field> fs = schema.getFields();
            //            for (int i = 0; i < backValueFields.length; i++) {
            //                Field f = fs.get(backValueFields[i]);
            //                Type type = f.schema().getType();
            //                if (type.compareTo(Type.ARRAY) == 0 | type.compareTo(Type.ENUM) == 0 | type.compareTo(Type.FIXED) == 0
            //                        | type.compareTo(Type.MAP) == 0 | type.compareTo(Type.NULL) == 0) {
            //                    throw new ClassCastException("This type is not supported: " + type);
            //                }
            //                fields.add(new Schema.Field(f.name(), f.schema(), null, null));
            //            }
            //            backValueSchema = Schema.createRecord(schema.getName(), null, null, false, fields);
            hasBackTree = hasBackTree(keyFields, backValueFields);
        } else {
            hasBackTree = !isHighest;
        }
        File p = new File(path);
        if (!p.exists())
            p.mkdirs();
        //        if (hasBackTree)
        //            this.backValueFields = backValueFields;
    }

    private boolean hasBackTree(int[] key, int[] value) {
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
        backValueofKeyFields = fields;
        return false;
    }

    public void create() {
        create(cachesize);
    }

    public Iterator<btree.Btree.Entry<KeyofBTree, String>> createIterator() {
        return btree.iterator();
    }

    /*
     * if the btree exists, create a new btree
     * else, read the btree file
     */
    public void create(int cacheSize) {
        File p = new File(path + "/file.db");
        if (p.exists()) {
            btree = new Btree<KeyofBTree, String>();
            btree.open(p.getPath());
            cachesize = btree.getCacheSize() - datacachesize;
            forwardCache = new HashMap<KeyofBTree, List<FlagKey>>();
        } else {
            if (!p.getParentFile().exists()) {
                p.getParentFile().mkdirs();
            }
            cachesize = cacheSize;
            fIn = 0;
            index = 0;
            cache = new ArrayList<Entry<KeyofBTree, String>>();
        }
    }

    public void createMerge(int cacheSize) {
        close();
        btree = null;
        NestManager.shDelete(path + "/file.db");
        NestManager.shDelete(path + "/file.db.meta.data");
        fIn = 0;
        cache = new ArrayList<Entry<KeyofBTree, String>>();
        cachesize = cacheSize;
        //        forwardCache.clear();
        forwardCache = null;
    }

    public void close() {
        if (btree != null)
            btree.close();
    }

    public void write() {
        if (fIn == 0) {
            mergeCache();
            index = 0;
        } else
            merge();
    }

    public void put(KeyofBTree key, int offset, KeyofBTree backKey, List<Record> forwardKey) {
        if (index >= MAX) {
            commit();
        }
        StringBuilder value = new StringBuilder();
        value.append(offset + "#");
        if (hasBackTree)
            value.append(backKey.toString());
        value.append("#");
        if (hasForwardTree)
            value.append(listTransToString(forwardKey));
        //        Entry v = new MyEntry<KeyofBTree, String>(key, value.toString());
        //System.out.println(key.toString());
        cache.add(new MyEntry<KeyofBTree, String>(key, value.toString()));
        index++;
    }

    public void commit() {
        if (index != 0) {
            try {
                BufferedWriter w = new BufferedWriter(new FileWriter(new File(path + "/file" + fIn)));
                fIn++;
                Collections.sort(cache, new EntryComparator<KeyofBTree, String>());
                for (Entry<KeyofBTree, String> en : cache) {
                    w.write(en.getKey().toString() + "@" + en.getValue() + "\n");
                }
                cache.clear();
                w.close();
                index = 0;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void merge() {
        if (index == 0)
            mergeFiles();
        else {
            mergeFilesAndCache();
            index = 0;
        }
    }

    public void mergeCache() {
        btree = new Btree<KeyofBTree, String>(nodenumofblock, path + "/file.db", cachesize, blocksize, datacachesize,
                0.6f);
        Collections.sort(cache, new EntryComparator<KeyofBTree, String>());
        Iterator<Entry<KeyofBTree, String>> it = cache.iterator();
        Entry<KeyofBTree, String> a1;
        while (it.hasNext()) {
            a1 = it.next();
            //System.out.println("<" + a1.getKey().toString());
            btree.insert(a1.getKey(), a1.getValue());
            //System.out.println(a1.getKey().toString() + "\t" + btree.find(a1.getKey()));
        }
        cache.clear();
        close();

        /*create();
        Iterator<Entry<KeyofBTree, String>> iter = cache.iterator();
        while (iter.hasNext()) {
            Entry<KeyofBTree, String> kv = iter.next();
            System.out.println(kv.getKey().toString() + "\t" + btree.find(kv.getKey()));
        }*/
    }

    public void mergeFilesAndCache() {
        btree = new Btree<KeyofBTree, String>(nodenumofblock, path + "/file.db", cachesize, blocksize, datacachesize,
                0.6f);
        Collections.sort(cache, new EntryComparator<KeyofBTree, String>());
        Iterator<Entry<KeyofBTree, String>> it = cache.iterator();
        Entry<KeyofBTree, String> a1 = it.next();
        File[] files = new File[fIn];
        for (int i = 0; i < fIn; i++) {
            files[i] = new File(path + "/file" + i);
        }
        SortedBTreeReader reader = new SortedBTreeReader(files);
        while (reader.hasNext()) {
            Entry<KeyofBTree, String> a2 = reader.next();
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
            file.delete();
        }
    }

    public void mergeFiles() {
        assert (fIn > 0);
        btree = new Btree<KeyofBTree, String>(nodenumofblock, path + "/file.db", cachesize, blocksize, datacachesize,
                0.6f);
        File[] files = new File[fIn];
        for (int i = 0; i < fIn; i++) {
            files[i] = new File(path + "/file" + i);
        }
        SortedBTreeReader reader = new SortedBTreeReader(files);
        while (reader.hasNext()) {
            Entry<KeyofBTree, String> en = reader.next();
            btree.insert(en.getKey(), en.getValue());
        }
        reader.close();
        btree.close();
        reader = null;
        btree = null;
        for (File file : files) {
            file.delete();
        }
    }

    public void insertForward(Record key, Record value) {
        insertForward(new KeyofBTree(key, keyFields.length), new KeyofBTree(value, forwardValueFields.length));
    }

    public void insertForward(KeyofBTree key, KeyofBTree value) {
        List<FlagKey> pr = forwardCache.get(key);
        if (pr == null) {
            List<FlagKey> v = new ArrayList<FlagKey>();
            v.add(new FlagKey(value));
            forwardCache.put(key, v);
        } else {
            if (!pr.isEmpty() && pr.get(0).getKClear() && pr.get(0).getKey() == null) {
                FlagKey v = pr.get(0);
                v.setKClear(false);
                pr.set(0, v);
                pr.add(new FlagKey(value));
            } else {
                pr.add(new FlagKey(value));
                Collections.sort(pr);
            }
            forwardCache.put(key, pr);
        }
    }

    public void insertForward(Record key, boolean isKey) {
        KeyofBTree k = isKey ? new KeyofBTree(key, keyFields.length) : new KeyofBTree(key, keyFields);
        forwardCache.put(k, new ArrayList<FlagKey>());
    }

    public void deleteForward(Record key, Record value) {
        deleteForward(new KeyofBTree(key, keyFields.length), new KeyofBTree(value, forwardValueFields.length));
    }

    public void deleteForward(KeyofBTree key, KeyofBTree value) {
        List<FlagKey> pr = forwardCache.get(key);
        if (pr == null) {
            pr = new ArrayList<FlagKey>();
            pr.add(new FlagKey(value));
        } else {
            boolean find = false;
            int i;
            for (i = 0; i < pr.size(); i++) {
                int com = pr.get(i).getKey().compareTo(value);
                if (com >= 0) {
                    if (com == 0)
                        find = true;
                    break;
                }
            }
            if (find)
                pr.remove(i);
            else
                pr.add(i, new FlagKey(value));
        }
        if (pr.isEmpty())
            forwardCache.remove(key);
        else
            forwardCache.put(key, pr);
    }

    public void deleteForward(Record key, boolean isKey) {
        KeyofBTree k = isKey ? new KeyofBTree(key, keyFields.length) : new KeyofBTree(key, keyFields);
        deleteForward(k);
    }

    public void deleteForward(KeyofBTree key) {
        List<FlagKey> pr = new ArrayList<FlagKey>();
        pr.add(new FlagKey());
        forwardCache.put(key, pr);
    }

    public String find(Record record, boolean isKey) {
        KeyofBTree key = isKey ? new KeyofBTree(record, keyFields.length) : new KeyofBTree(record, keyFields);
        return find(key);
    }

    public String find(KeyofBTree key) {
        return btree.find(key);
    }

    public List<Record> findForward(KeyofBTree key) {
        List<FlagKey> pr = forwardCache.get(key);
        List<Record> rs = findForwardDiskRecord(key);
        return mergeRecordList(rs, pr);
    }

    public List<Record> findForwardDiskRecord(KeyofBTree key) {
        return stringTransToList(findForwardDisk(key));
    }

    public String findForwardDisk(KeyofBTree key) {
        return btree.find(key).split("#")[2];
    }

    public Integer findOffset(Record record, int[] fields) {
        return findOffset(new KeyofBTree(record, fields));
    }

    public Integer findOffset(Record record, boolean isKey) {
        KeyofBTree key = isKey ? new KeyofBTree(record, keyFields.length) : new KeyofBTree(record, keyFields);
        return findOffset(key);
    }

    public Integer findOffset(KeyofBTree key) {
        String m = btree.find(key);
        if (m == null)
            return null;
        return Integer.parseInt(m.split("#")[0]);
    }

    public KeyofBTree findBackKey(KeyofBTree key) {
        if (isHighest)
            return null;
        if (!hasBackTree)
            return key.get(backValueofKeyFields);
        String m = btree.find(key);
        if (m == null)
            return null;
        return new KeyofBTree(m.split("#")[1]);
    }

    private List<Record> mergeRecordList(List<Record> rs, List<FlagKey> fs) {
        if (fs == null)
            return rs;
        List<Record> res = new ArrayList<Record>();
        if (rs == null)
            rs = new ArrayList<Record>();
        if (fs.isEmpty())
            return rs;
        Iterator<FlagKey> it = fs.iterator();
        int in = 0;
        FlagKey ew = it.next();
        if (ew.getKey() == null) {
            if (ew.getKClear())
                return null;
            else
                rs.clear();
        } else {
            while (in < rs.size() && ew.getKey().compareTo(new KeyofBTree(rs.get(in))) > 0) {
                res.add(rs.get(in));
                in++;
            }
            if (in >= rs.size())
                res.add(keyTransToRecord(ew.getKey(), forwardValueSchema));
            else if (ew.getKey().compareTo(new KeyofBTree(rs.get(in))) == 0)
                in++;
            else
                res.add(keyTransToRecord(ew.getKey(), forwardValueSchema));
        }
        while (it.hasNext()) {
            ew = it.next();
            while (in < rs.size() && ew.getKey().compareTo(new KeyofBTree(rs.get(in))) > 0) {
                res.add(rs.get(in));
                in++;
            }
            if (in >= rs.size())
                res.add(keyTransToRecord(ew.getKey(), forwardValueSchema));
            else if (ew.getKey().compareTo(new KeyofBTree(rs.get(in))) == 0)
                in++;
            else
                res.add(keyTransToRecord(ew.getKey(), forwardValueSchema));
        }
        while (in < rs.size()) {
            res.add(rs.get(in));
            in++;
        }
        return res;
    }

    private String listTransToString(List<Record> rs) {
        StringBuilder v = new StringBuilder();
        if (rs.size() > 0)
            v.append(NestManager.writeKeyRecord(rs.get(0)));
        if (rs.size() > 1) {
            for (int i = 1; i < rs.size(); i++) {
                v.append("&");
                v.append(NestManager.writeKeyRecord(rs.get(i)));
            }
        }
        return v.toString();
    }

    private List<Record> stringTransToList(String v) {
        List<Record> rs = new ArrayList<Record>();
        if (v == null)
            return null;
        if (v.equals(""))
            return rs;
        String[] tm = v.split("&");
        for (String t : tm) {
            rs.add(NestManager.readKeyRecord(forwardValueSchema, t));
        }
        return rs;
    }

    private Record keyTransToRecord(KeyofBTree key, Schema s) {
        Record r = new Record(s);
        for (int i = 0; i < key.getLength(); i++) {
            r.put(i, key.values[i]);
        }
        return r;
    }

    class FlagKey implements Comparable<FlagKey> {
        KeyofBTree key;
        boolean kClear;

        public FlagKey() {
            key = null;
            kClear = true;
        }

        public FlagKey(KeyofBTree key) {
            this.key = key;
            kClear = false;
        }

        public KeyofBTree getKey() {
            return key;
        }

        public void setKey(Record key) {
            setKey(new KeyofBTree(key));
        }

        public void setKey(KeyofBTree key) {
            this.key = key;
        }

        public void setKClear(boolean e) {
            kClear = e;
        }

        public boolean getKClear() {
            return kClear;
        }

        @Override
        public int compareTo(FlagKey o) {
            if (key == null) {
                if (o.getKey() == null)
                    return 0;
                else
                    return 1;
            } else {
                if (o.getKey() == null)
                    return -1;
                else
                    return key.compareTo(o.getKey());
            }
        }
    }

    public class SortedBTreeReader implements Closeable {
        private BufferedReader[] reader;
        private Entry<KeyofBTree, String>[] values;
        private int[] sort;
        private int start;

        public SortedBTreeReader(File[] files) {
            reader = new BufferedReader[files.length];
            int i = 0;
            try {
                for (File file : files) {
                    reader[i] = new BufferedReader(new FileReader(file));
                    i++;
                }

                values = new Entry[files.length];
                for (i = 0; i < values.length; i++) {
                    String[] tmp = reader[i].readLine().split("@");
                    values[i] = new MyEntry<KeyofBTree, String>(new KeyofBTree(tmp[0]), tmp[1]);
                }
            } catch (IOException e) {
                e.printStackTrace();
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

        private void read(int i) {
            try {
                String line;
                if ((line = reader[i].readLine()) != null) {
                    String[] tmp = line.split("@");
                    values[i] = new MyEntry<KeyofBTree, String>(new KeyofBTree(tmp[0]), tmp[1]);
                } else
                    values[i] = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public Entry<KeyofBTree, String> next() {
            Entry<KeyofBTree, String> res = values[sort[start]];
            read(sort[start]);
            if (values[sort[start]] == null) {
                start++;
            } else {
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

        @Override
        public void close() {
            try {
                for (int i = 0; i < reader.length; i++) {
                    reader[i].close();
                }
                reader = null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

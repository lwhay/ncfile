package org.apache.trevni.avro.update;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import btree.Btree;

public class ForwardTree {
    private int[] keyFields;
    private int[] valueFields;
    private Schema schema;
    private Schema valueSchema;
    private String path;
    private Btree<KeyofBTree, String> btree;
    private Btree<KeyofBTree, String> tmpBtree;
    //    private List<Entry<KeyofBTree, String>> hash;
    //    private int index;
    //    private int fIn;

    //    private static final int MAX = 500000;
    final int blocksize = 512;
    private int cachesize = 3000;
    final int datacachesize = 1000;
    final int nodenumofblock = 1;
    private Iterator<Btree.Entry<KeyofBTree, String>> btreeIter;
    private Btree.Entry<KeyofBTree, String> en1;
    //    private int index;
    private HashMap<KeyofBTree, List<FlagCombKey>> cache;
    private Iterator<Entry<KeyofBTree, List<FlagCombKey>>> cacheIter;
    private Entry<KeyofBTree, List<FlagCombKey>> en2;

    private int in;

    public ForwardTree(int[] keyFields, int[] valueFields, Schema schema, Schema valueSchema, String path) {
        this.keyFields = keyFields;
        this.valueFields = valueFields;
        this.schema = schema;
        this.valueSchema = valueSchema;
        this.path = path;
        //        create();
        //        index = 0;
    }

    public void create() {
        create(cachesize);
    }

    /*
     * if the btree exists, create a new btree
     * else, read the btree file
     */
    public void create(int cacheSize) {
        cache = new HashMap<KeyofBTree, List<FlagCombKey>>();
        File p = new File(path + "/file.db");
        if (p.exists()) {
            btree = new Btree<KeyofBTree, String>();
            btree.open(p.getPath());
            cachesize = btree.getCacheSize() - datacachesize;
        } else {
            if (!p.getParentFile().exists()) {
                p.getParentFile().mkdirs();
            }
            cachesize = cacheSize;
            btree = new Btree<KeyofBTree, String>(nodenumofblock, p.getPath(), cachesize, blocksize, datacachesize,
                    0.6f);
            //            fIn = 0;
            //            hash = new ArrayList<Entry<KeyofBTree, String>>();
        }
    }

    public void createIterator() {
        //        create();
        btree.close();
        btree = new Btree<KeyofBTree, String>();
        btree.open((path + "/file.db"), 10, 10);
        btreeIter = btree.iterator();
    }

    public void createMerge(int cacheSize) {
        cachesize = cacheSize;
        tmpBtree = new Btree<KeyofBTree, String>(nodenumofblock, path + "/tmp", cachesize, blocksize, datacachesize,
                0.6f);
        //        btree.close();
        //        btree = new Btree<KeyofBTree, String>();
        //        btree.open(file.getPath(), 10, 10);
        //        btreeIter = btree.iterator();
        createIterator();
        if (btreeIter.hasNext())
            en1 = btreeIter.next();
        if (!cache.isEmpty()) {
            List<Entry<KeyofBTree, List<FlagCombKey>>> list = new ArrayList<Entry<KeyofBTree, List<FlagCombKey>>>();
            list.addAll(cache.entrySet());
            Collections.sort(list, new EntryComparator<KeyofBTree, List<FlagCombKey>>());
            cacheIter = list.iterator();
            if (cacheIter.hasNext())
                en2 = cacheIter.next();
            in = 0;
        } else
            in = 1;
    }

    public boolean hasNext() {
        return btreeIter.hasNext();
    }

    public Btree.Entry<KeyofBTree, String> next() {
        return btreeIter.next();
    }

    /*
     * write the head node to the meta file, and close the btree
     */
    public void close() {
        btree.close();
    }

    public void put(Record key, List<Record> value) {
        String v = listTransToString(value);
        btree.insert(new KeyofBTree(new CombKey(key, keyFields.length)), v);
        //        hash.add(new MyEntry<KeyofBTree, String>(new KeyofBTree(new CombKey(key, keyFields.length)), v.toString()));
        //        index++;
        //        if (index >= MAX)
        //            commit();
        //        btree.insert(new KeyofBTree(new CombKey(data, keyFields[0].length)), NestManager.writeKeyRecord(data));
    }

    public void insert(Record key, Record value) {
        insert(new CombKey(key, keyFields.length), new CombKey(value, valueFields.length));
    }

    public void insert(CombKey key, CombKey value) {
        KeyofBTree k = new KeyofBTree(key);
        List<FlagCombKey> pr = cache.get(k);
        if (pr == null) {
            List<FlagCombKey> v = new ArrayList<FlagCombKey>();
            v.add(new FlagCombKey(value));
            cache.put(k, v);
        } else {
            if (!pr.isEmpty() && pr.get(0).getKClear() && pr.get(0).getKey() == null) {
                FlagCombKey v = pr.get(0);
                v.setKClear(false);
                pr.set(0, v);
                pr.add(new FlagCombKey(value));
            } else {
                pr.add(new FlagCombKey(value));
                Collections.sort(pr);
            }
            cache.put(k, pr);
        }
    }

    public void insert(Record key, boolean isKey) {
        KeyofBTree k = isKey ? new KeyofBTree(new CombKey(key, keyFields.length))
                : new KeyofBTree(new CombKey(key, keyFields));
        cache.put(k, new ArrayList<FlagCombKey>());
    }

    public void delete(Record key, Record value) {
        delete(new CombKey(key, keyFields.length), new CombKey(value, valueFields.length));
    }

    public void delete(CombKey key, CombKey value) {
        KeyofBTree k = new KeyofBTree(key);
        List<FlagCombKey> pr = cache.get(k);
        if (pr == null) {
            pr = new ArrayList<FlagCombKey>();
            pr.add(new FlagCombKey(value));
        } else {
            //            CombKey ew = new CombKey(value, valueFields.length);
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
                pr.add(i, new FlagCombKey(value));
            //            Collections.sort(pr);
        }
        if (pr.isEmpty())
            cache.remove(k);
        else
            cache.put(k, pr);
    }

    public void delete(Record key, boolean isKey) {
        KeyofBTree k = isKey ? new KeyofBTree(new CombKey(key, keyFields.length))
                : new KeyofBTree(new CombKey(key, keyFields));
        delete(k);
    }

    public void delete(CombKey key) {
        delete(new KeyofBTree(key));
    }

    public void delete(KeyofBTree key) {
        List<FlagCombKey> pr = new ArrayList<FlagCombKey>();
        pr.add(new FlagCombKey());
        cache.put(key, pr);
    }

    //    private void commit() {
    //        if (index != 0) {
    //            btree = new Btree<KeyofBTree, String>(nodenumofblock, path + "/file" + fIn + ".db", cachesize, blocksize,
    //                    datacachesize, 0.6f);
    //            fIn++;
    //            Collections.sort(hash, new EntryComparator<KeyofBTree, String>());
    //            for (Entry<KeyofBTree, String> en : hash) {
    //                btree.insert(en.getKey(), en.getValue());
    //            }
    //            hash.clear();
    //            btree.close();
    //            index = 0;
    //        }
    //    }

    /*
     * find the key in disk
     */
    //    public Record getRecord(CombKey key) {
    //        return NestManager.readKeyRecord(schema[0], btree.find(new KeyofBTree(key)).toString());
    //    }

    public List<Record> find(CombKey key) {
        return find(new KeyofBTree(key));
    }

    public List<Record> find(KeyofBTree key) {
        List<FlagCombKey> pr = cache.get(key);
        List<Record> rs = findDiskRecord(key);
        return mergeRecordList(rs, pr);
    }

    public List<Record> findDiskRecord(KeyofBTree key) {
        return stringTransToList(btree.find(key));
    }

    public String findDisk(CombKey key) {
        return btree.find(new KeyofBTree(key));
    }

    private List<Record> mergeRecordList(List<Record> rs, List<FlagCombKey> fs) {
        if (fs == null)
            return rs;
        List<Record> res = new ArrayList<Record>();
        if (rs == null)
            rs = new ArrayList<Record>();
        if (fs.isEmpty())
            return rs;
        Iterator<FlagCombKey> it = fs.iterator();
        int in = 0;
        FlagCombKey ew = it.next();
        if (ew.getKey() == null) {
            if (ew.getKClear())
                return null;
            else
                rs.clear();
        } else {
            while (in < rs.size() && ew.getKey().compareTo(new CombKey(rs.get(in))) > 0) {
                res.add(rs.get(in));
                in++;
            }
            if (in >= rs.size())
                res.add(keyTransToRecord(ew.getKey(), valueSchema));
            else if (ew.getKey().compareTo(new CombKey(rs.get(in))) == 0)
                in++;
            else
                res.add(keyTransToRecord(ew.getKey(), valueSchema));
        }
        while (it.hasNext()) {
            ew = it.next();
            while (in < rs.size() && ew.getKey().compareTo(new CombKey(rs.get(in))) > 0) {
                res.add(rs.get(in));
                in++;
            }
            if (in >= rs.size())
                res.add(keyTransToRecord(ew.getKey(), valueSchema));
            else if (ew.getKey().compareTo(new CombKey(rs.get(in))) == 0)
                in++;
            else
                res.add(keyTransToRecord(ew.getKey(), valueSchema));
        }
        while (in < rs.size()) {
            res.add(rs.get(in));
            in++;
        }
        return res;
    }

    private List<Record> stringTransToList(String v) {
        List<Record> rs = new ArrayList<Record>();
        if (v == null)
            return null;
        if (v.equals(""))
            return rs;
        String[] tm = v.split("&");
        for (String t : tm) {
            rs.add(NestManager.readKeyRecord(valueSchema, t));
        }
        return rs;
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

    private Record keyTransToRecord(CombKey key, Schema s) {
        Record r = new Record(s);
        for (int i = 0; i < key.getLength(); i++) {
            r.put(i, key.get(i));
        }
        return r;
    }

    //    public Record getRecord(Record data, boolean isKey) {
    //        return isKey ? getRecord(new CombKey(data)) : getRecord(new CombKey(data, keyFields[0]));
    //    }

    /*
     * find the forward key value in cache and disk
      */
    //    public Record find(Record data, boolean isKey) {
    //        CombKey key = isKey ? new CombKey(data) : new CombKey(data, keyFields[0]);
    //        PairRecord ch = cache.get(key);
    //        Record re = getRecord(key);
    //        return re;
    //    }

    /*
     * merge the foreign key caches to disk and clear the cache
     */
    public Entry<CombKey, List<Record>> nextMerge() {
        Entry<CombKey, List<Record>> res;
        while (true) {
            if (in >= 2) {
                cache.clear();
                //            index = 0;
                tmpBtree.close();
                btree.close();
                btree = null;
                btreeIter = null;
                cacheIter = null;
                //                new File(path + "/file.db").delete();
                //                new File(path + "/file.db.meta.data").delete();
                NestManager.shDelete(path + "/file.db");
                NestManager.shDelete(path + "/file.db.meta.data");
                new File(path + "/tmp").renameTo(new File(path + "/file.db"));
                new File(path + "/tmp.meta.data").renameTo(new File(path + "/file.db.meta.data"));
                return null;
            } else if (in == 0) {
                int comp = en1.getKey().compareTo(en2.getKey());
                if (comp < 0) {
                    res = new MyEntry<CombKey, List<Record>>(new CombKey(en1.getKey()),
                            stringTransToList(en1.getValue()));
                    tmpBtree.insert(en1.getKey(), en1.getValue());
                    if (btreeIter.hasNext())
                        en1 = btreeIter.next();
                    else {
                        en1 = null;
                        in++;
                    }
                } else if (comp == 0) {
                    //                res = en2;
                    //                if (en2.getValue() != null)
                    //                    tmpBtree.insert(new KeyofBTree(en2.getKey()), NestManager.writeKeyRecord(en2.getValue()));
                    List<Record> rr = mergeRecordList(stringTransToList(en1.getValue()), en2.getValue());
                    if (rr == null) {
                        if (btreeIter.hasNext())
                            en1 = btreeIter.next();
                        else {
                            en1 = null;
                            in++;
                        }
                        if (cacheIter.hasNext())
                            en2 = cacheIter.next();
                        else {
                            en2 = null;
                            in++;
                        }
                        continue;
                    }
                    res = new MyEntry<CombKey, List<Record>>(new CombKey(en1.getKey()), rr);
                    tmpBtree.insert(en1.getKey(), listTransToString(rr));
                    if (btreeIter.hasNext())
                        en1 = btreeIter.next();
                    else {
                        en1 = null;
                        in++;
                    }
                    if (cacheIter.hasNext())
                        en2 = cacheIter.next();
                    else {
                        en2 = null;
                        in++;
                    }
                } else {
                    List<Record> rr = mergeRecordList(null, en2.getValue());
                    if (rr == null) {
                        if (cacheIter.hasNext())
                            en2 = cacheIter.next();
                        else {
                            en2 = null;
                            in++;
                        }
                        continue;
                    }
                    res = new MyEntry<CombKey, List<Record>>(new CombKey(en2.getKey()), rr);
                    tmpBtree.insert(en2.getKey(), listTransToString(rr));
                    //                if (en2.getValue() != null)
                    //                    tmpBtree.insert(new KeyofBTree(en2.getKey()), NestManager.writeKeyRecord(en2.getValue()));
                    if (cacheIter.hasNext())
                        en2 = cacheIter.next();
                    else {
                        en2 = null;
                        in++;
                    }
                }
                break;
            } else {
                if (en1 != null) {
                    res = new MyEntry<CombKey, List<Record>>(new CombKey(en1.getKey()),
                            stringTransToList(en1.getValue()));
                    tmpBtree.insert(en1.getKey(), en1.getValue());
                    if (btreeIter.hasNext())
                        en1 = btreeIter.next();
                    else {
                        en1 = null;
                        in++;
                    }
                } else {
                    List<Record> rr = mergeRecordList(null, en2.getValue());
                    if (rr == null) {
                        if (cacheIter.hasNext())
                            en2 = cacheIter.next();
                        else {
                            en2 = null;
                            in++;
                        }
                        continue;
                    }
                    res = new MyEntry<CombKey, List<Record>>(new CombKey(en2.getKey()), rr);
                    tmpBtree.insert(en2.getKey(), listTransToString(rr));
                    if (cacheIter.hasNext())
                        en2 = cacheIter.next();
                    else {
                        en2 = null;
                        in++;
                    }
                }
                break;
            }
        }
        return res;
    }

    public void merge(int num) {
        if (cache.isEmpty())
            return;
        createMerge((int) (num / 500));
        while (en2 != null) {
            while (en1 != null && en1.getKey().compareTo(en2.getKey()) < 0) {
                tmpBtree.insert(en1.getKey(), en1.getValue());
                if (btreeIter.hasNext())
                    en1 = btreeIter.next();
                else
                    en1 = null;
            }
            if (en1 == null)
                break;
            if (en1.getKey().compareTo(en2.getKey()) == 0) {
                List<Record> rr = mergeRecordList(stringTransToList(en1.getValue()), en2.getValue());
                if (rr != null)
                    tmpBtree.insert(en1.getKey(), listTransToString(rr));
                if (btreeIter.hasNext())
                    en1 = btreeIter.next();
                else
                    en1 = null;
            } else {
                List<Record> rr = mergeRecordList(null, en2.getValue());
                tmpBtree.insert(en1.getKey(), listTransToString(rr));
            }
            if (cacheIter.hasNext())
                en2 = cacheIter.next();
            else
                en2 = null;
        }

        while (en2 != null) {
            List<Record> rr = mergeRecordList(null, en2.getValue());
            tmpBtree.insert(en1.getKey(), listTransToString(rr));
            if (cacheIter.hasNext())
                en2 = cacheIter.next();
            else
                en2 = null;
        }

        while (en1 != null) {
            tmpBtree.insert(en1.getKey(), en1.getValue());
            if (btreeIter.hasNext())
                en1 = btreeIter.next();
            else
                en1 = null;
        }
        tmpBtree.close();
        btreeIter = null;
        cacheIter = null;
        btree.close();
        //        new File(path + "/file.db").delete();
        //        new File(path + "/file.db.meta.data").delete();
        NestManager.shDelete(path + "/file.db");
        NestManager.shDelete(path + "/file.db.meta.data");
        new File(path + "/tmp").renameTo(new File(path + "/file.db"));
        new File(path + "/tmp.meta.data").renameTo(new File(path + "/file.db.meta.data"));
    }
}

class FlagCombKey implements Comparable<FlagCombKey> {
    CombKey key;
    boolean kClear;

    public FlagCombKey() {
        key = null;
        kClear = true;
    }

    public FlagCombKey(CombKey key) {
        this.key = key;
        kClear = false;
    }

    public CombKey getKey() {
        return key;
    }

    public void setKey(Record key) {
        setKey(new CombKey(key));
    }

    public void setKey(CombKey key) {
        this.key = key;
    }

    public void setKClear(boolean e) {
        kClear = e;
    }

    public boolean getKClear() {
        return kClear;
    }

    @Override
    public int compareTo(FlagCombKey o) {
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

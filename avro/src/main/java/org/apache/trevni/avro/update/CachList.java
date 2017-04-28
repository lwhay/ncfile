package org.apache.trevni.avro.update;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.avro.generic.GenericData.Record;

public class CachList {
    //    private List<FlagData> cach;
    private static int[][] keyFields;
    private int level;
    private int size;
    private HashMap<CombKey, FlagData>[] hash;
    private HashMap<CombKey, FlagData>[] extraHash;
    private Iterator<Entry<CombKey, FlagData>>[] mergeIter;
    private Iterator<Entry<CombKey, FlagData>>[] extraMergeIter;
    private List<Entry<Integer, FlagData>>[] mergeList;
    //    private HashMap<NestCombKey, FlagData>[] sortMap;
    private List<Entry<NestCombKey, FlagData>>[] sortList;
    private Iterator<Entry<NestCombKey, FlagData>>[] sortIter;
    private int max;
    private int index;

    public CachList(int[][] keyFields) {
        //        cach = new ArrayList<FlagData>();
        this.keyFields = keyFields;
        level = keyFields.length;
        hash = new HashMap[level];
        extraHash = new HashMap[level - 1];
        for (int i = 0; i < (level - 1); i++) {
            hash[i] = new HashMap<CombKey, FlagData>();
            extraHash[i] = new HashMap<CombKey, FlagData>();
        }
        hash[level - 1] = new HashMap<CombKey, FlagData>();
        max = -1;
    }

    public void setMAX(int max) {
        this.max = max;
    }

    public boolean isEmpty() {
        return (size == 0);
    }

    public boolean isFull() {
        if (max == -1)
            return false;
        return size >= max;
    }

    public int size() {
        return size;
    }

    public void clear() {
        for (List<Entry<Integer, FlagData>> m : mergeList)
            m.clear();
        mergeList = null;
        size = 0;
    }

    public void add(Record data, byte flag, int le) {
        add(new FlagData(flag, data, le));
    }

    public void add(Record data, byte flag, int le, int ne) {
        add(ne, new FlagData(flag, data, le));
    }

    public void extraAdd(Record data, byte flag, int le) {
        extraAdd(new FlagData(flag, data, le));
    }

    public void extraAdd(FlagData fd) {
        extraHash[fd.getLevel() - 1].put(new CombKey(fd.getData(), keyFields[fd.getLevel()]), fd);
        //        cach.add(fd);
        size++;
    }

    public void add(int le, FlagData fd) {
        CombKey key = new CombKey(fd.getData(), keyFields[le].length);
        fd.setData(null);
        hash[le].put(key, fd);
        size++;
    }

    public void add(int le, CombKey key, FlagData fd) {
        hash[le].put(key, fd);
        size++;
    }

    public void add(FlagData fd) {
        //        if (fd.getFlag() == (byte) 2) {
        //            deleteHash(fd.getData(), fd.getLevel());
        //        } else {
        if (fd.flag == (byte) 2) {
            CombKey key = new CombKey(fd.getData(), keyFields[fd.getLevel()]);
            fd.setData(null);
            hash[fd.getLevel()].put(key, fd);
        } else
            hash[fd.getLevel()].put(new CombKey(fd.getData(), keyFields[fd.getLevel()]), fd);

        //        }
        //        cach.add(fd);
        size++;
    }

    //    private void deleteHash(Record data, int le) {
    //        assert (le < level);
    //        hash[le].put(new CombKey(data, keyFields[le]), new FlagData((byte) 2, data, le));
    //        size++;
    //        if (le < (level - 1)) {
    //            int i = data.getSchema().getFields().size();
    //            for (Record re : (List<Record>) data.get((i - 1))) {
    //                deleteHash(re, (le + 1));
    //            }
    //        }
    //    }

    public void delete(Record data, int le, boolean isKey) {
        if (isKey)
            delete(new CombKey(data), le);
        else
            delete(new CombKey(data, keyFields[le]), le);
    }

    public void delete(CombKey key, int le) {
        hash[le].remove(key);
        size--;
    }

    public void extraDelete(Record data, int le, boolean isKey) {
        if (isKey)
            extraDelete(new CombKey(data), le);
        else
            extraDelete(new CombKey(data, keyFields[le]), le);
    }

    public void extraDelete(CombKey key, int le) {
        extraHash[le - 1].remove(key);
        size--;
    }

    public FlagData find(Record data, int[] fields, int le) {
        return find(new CombKey(data, fields), le);
    }

    public FlagData find(Record data, int le, boolean isKey) {
        return isKey ? find(new CombKey(data), le) : find(new CombKey(data, keyFields[le]), le);
    }

    public FlagData find(CombKey key, int le) {
        return hash[le].get(key);
    }

    public FlagData extraFind(Record data, int[] fields, int le) {
        return extraFind(new CombKey(data, fields), le);
    }

    public FlagData extraFind(Record data, int le, boolean isKey) {
        return isKey ? extraFind(new CombKey(data), le) : extraFind(new CombKey(data, keyFields[le]), le);
    }

    public FlagData extraFind(CombKey key, int le) {
        return extraHash[le - 1].get(key);
    }

    public int getMergeLength(int i) {
        return mergeList[i].size();
    }

    public int getMergePlace(int le, int index) {
        return mergeList[le].get(index).getKey();
    }

    public void mergeCreate() {
        mergeIter = new Iterator[level];
        extraMergeIter = new Iterator[level - 1];
        mergeList = new List[level];
        //        sortMap = new HashMap[level];
        sortList = new List[level];
        mergeIter[0] = hash[0].entrySet().iterator();
        mergeList[0] = new ArrayList<Entry<Integer, FlagData>>();
        //        sortMap[0] = new HashMap<NestCombKey, FlagData>();
        sortList[0] = new ArrayList<Entry<NestCombKey, FlagData>>();

        for (int i = 1; i < level; i++) {
            mergeIter[i] = hash[i].entrySet().iterator();
            extraMergeIter[i - 1] = extraHash[i - 1].entrySet().iterator();
            mergeList[i] = new ArrayList<Entry<Integer, FlagData>>();
            //            sortMap[i] = new HashMap<NestCombKey, FlagData>();
            sortList[i] = new ArrayList<Entry<NestCombKey, FlagData>>();
        }
    }

    public void mergeWriteCreate() {
        index = 0;
    }

    public Entry<Integer, FlagData> mergeNext(int le) {
        if (index >= mergeList[le].size())
            return null;
        Entry<Integer, FlagData> res = mergeList[le].get(index);
        index++;
        return res;
    }

    public void sortSortList(int le) {
        Collections.sort(sortList[le], new EntryComparator<NestCombKey, FlagData>());
    }

    public void sortMergeList(int le) {
        Collections.sort(mergeList[le], new EntryComparator<Integer, FlagData>());
    }

    public void createSortIterator() {
        sortIter = new Iterator[level];
        for (int i = 0; i < level; i++) {
            sortIter[i] = sortList[i].iterator();
        }
    }

    public boolean hasNextSort(int le) {
        return sortIter[le].hasNext();
    }

    public Entry<NestCombKey, FlagData> nextSort(int le) {
        return sortIter[le].next();
    }

    public boolean hasNext(int le) {
        return mergeIter[le].hasNext();
    }

    public void hashClear() {
        for (HashMap<CombKey, FlagData> hh : hash)
            hh.clear();
        for (HashMap<CombKey, FlagData> hh : extraHash)
            hh.clear();
        mergeIter = null;
        extraMergeIter = null;
    }

    public void sortClear(int le) {
        sortList[le].clear();
    }

    public void sortClear() {
        sortList = null;
        sortIter = null;
    }

    public boolean extraHasNext(int le) {
        assert (le > 0);
        return extraMergeIter[le - 1].hasNext();
    }

    public Entry<CombKey, FlagData> next(int le) {
        return mergeIter[le].next();
    }

    public Entry<CombKey, FlagData> extraNext(int le) {
        return extraMergeIter[le - 1].next();
    }

    public void remove(int le) {
        mergeIter[le].remove();
    }

    public void extraRemove(int le) {
        extraMergeIter[le - 1].remove();
    }

    public void addToSortList(NestCombKey key, FlagData value, int le) {
        sortList[le].add(new MyEntry<NestCombKey, FlagData>(key, value));
    }

    public void addToMergeList(int place, FlagData value, int le) {
        mergeList[le].add(new MyEntry<Integer, FlagData>(place, value));
    }

    /*
     * find the nest foreign key in the up level sortList, assure the sortList is sorted by nest key
     * if not find, return null
     */
    public NestCombKey findSort(CombKey key, int le) {
        int i = 0;
        int j = sortList[le].size() - 1;
        while (i != j) {
            int middle = (i + j) / 2;
            NestCombKey res = sortList[le].get(middle).getKey();
            int c = res.compareTo(key, le);
            if (c == 0)
                return res;
            else if (c > 0)
                i = middle;
            else
                j = middle;
        }
        NestCombKey res = sortList[le].get(i).getKey();
        int c = res.compareTo(key, le);
        if (c == 0)
            return res;
        else
            return null;
    }

    public static class FlagData implements Comparable<CachList.FlagData> {
        private byte flag; //0 null; 1 insert; 2 delete; 3 update
        private Record data;
        private int level;

        public FlagData(byte flag, Record data, int level) {
            this.flag = flag;
            this.data = data;
            this.level = level;
        }

        public int compareTo(FlagData o) {
            int re = 0;
            if (level == o.getLevel()) {
                for (int i = 0; i < level; i++) {
                    re = new ComparableKey(data, keyFields[i]).compareTo(new ComparableKey(o.getData(), keyFields[i]));
                    if (re != 0) {
                        return re;
                    }
                }
            } else {
                int le = level < o.level ? level : o.getLevel();
                for (int i = 0; i < le; i++) {
                    re = new ComparableKey(data, keyFields[i]).compareTo(new ComparableKey(o.getData(), keyFields[i]));
                    if (re != 0) {
                        break;
                    }
                }
                if (re == 0) {
                    re = level < o.getLevel() ? -1 : 1;
                }
            }
            return re;
        }

        public void setFlag(byte flag) {
            this.flag = flag;
        }

        public void setData(Record data) {
            this.data = data;
        }

        public void setLevel(int level) {
            this.level = level;
        }

        public byte getFlag() {
            return flag;
        }

        public Record getData() {
            return data;
        }

        public int getLevel() {
            return level;
        }

        public String toString() {
            return level + "|" + flag + "|" + data.toString();
        }
    }
}

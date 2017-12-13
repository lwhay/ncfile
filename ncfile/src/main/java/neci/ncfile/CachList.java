package neci.ncfile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import neci.ncfile.generic.GenericData.Record;

public class CachList {
    //    private List<FlagData> cach;
    private static int[][] keyFields;
    private int layer;
    private int size;
    private HashMap<KeyofBTree, FlagData>[] hash;
    private HashMap<KeyofBTree, FlagData>[] extraHash;
    private Iterator<Entry<KeyofBTree, FlagData>>[] mergeIter;
    private Iterator<Entry<KeyofBTree, FlagData>>[] extraMergeIter;
    private List<Entry<Integer, FlagData>>[] mergeList;
    //    private HashMap<NestCombKey, FlagData>[] sortMap;
    private List<Entry<NestCombKey, FlagData>>[] sortList;
    private Iterator<Entry<NestCombKey, FlagData>>[] sortIter;
    private int max;
    private int index;

    public CachList(int[][] keyFields) {
        //        cach = new ArrayList<FlagData>();
        this.keyFields = keyFields;
        layer = keyFields.length;
        hash = new HashMap[layer];
        extraHash = new HashMap[layer - 1];
        for (int i = 0; i < (layer - 1); i++) {
            hash[i] = new HashMap<KeyofBTree, FlagData>();
            extraHash[i] = new HashMap<KeyofBTree, FlagData>();
        }
        hash[layer - 1] = new HashMap<KeyofBTree, FlagData>();
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

    public long getBytesSize() {
        long res = 0;
        Iterator<Entry<KeyofBTree, FlagData>> iter;
        for (int i = 0; i < layer; i++) {
            iter = hash[i].entrySet().iterator();
            while (iter.hasNext()) {
                Entry<KeyofBTree, FlagData> next = iter.next();
                res += next.getKey().getBytesSize();
                res += next.getValue().getBytesSize();
            }
            iter = extraHash[i].entrySet().iterator();
            while (iter.hasNext()) {
                Entry<KeyofBTree, FlagData> next = iter.next();
                res += next.getKey().getBytesSize();
                res += next.getValue().getBytesSize();
            }
        }
        return res;
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
        extraHash[fd.getLevel() - 1].put(new KeyofBTree(fd.getData(), keyFields[fd.getLevel()]), fd);
        //        cach.add(fd);
        size++;
    }

    public void add(int le, FlagData fd) {
        KeyofBTree key = new KeyofBTree(fd.getData(), keyFields[le].length);
        fd.setData(null);
        hash[le].put(key, fd);
        size++;
    }

    public void add(int le, KeyofBTree key, FlagData fd) {
        hash[le].put(key, fd);
        size++;
    }

    public void add(FlagData fd) {
        //        if (fd.getFlag() == (byte) 2) {
        //            deleteHash(fd.getData(), fd.getLevel());
        //        } else {
        if (fd.flag == (byte) 2) {
            KeyofBTree key = new KeyofBTree(fd.getData(), keyFields[fd.getLevel()]);
            fd.setData(null);
            hash[fd.getLevel()].put(key, fd);
        } else
            hash[fd.getLevel()].put(new KeyofBTree(fd.getData(), keyFields[fd.getLevel()]), fd);

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
            delete(new KeyofBTree(data), le);
        else
            delete(new KeyofBTree(data, keyFields[le]), le);
    }

    public void delete(KeyofBTree key, int le) {
        hash[le].remove(key);
        size--;
    }

    public void extraDelete(Record data, int le, boolean isKey) {
        if (isKey)
            extraDelete(new KeyofBTree(data), le);
        else
            extraDelete(new KeyofBTree(data, keyFields[le]), le);
    }

    public void extraDelete(KeyofBTree key, int le) {
        extraHash[le - 1].remove(key);
        size--;
    }

    public FlagData find(Record data, int[] fields, int le) {
        return find(new KeyofBTree(data, fields), le);
    }

    public FlagData find(Record data, int le, boolean isKey) {
        return isKey ? find(new KeyofBTree(data), le) : find(new KeyofBTree(data, keyFields[le]), le);
    }

    public FlagData find(KeyofBTree key, int le) {
        return hash[le].get(key);
    }

    public FlagData extraFind(Record data, int[] fields, int le) {
        return extraFind(new KeyofBTree(data, fields), le);
    }

    public FlagData extraFind(Record data, int le, boolean isKey) {
        return isKey ? extraFind(new KeyofBTree(data), le) : extraFind(new KeyofBTree(data, keyFields[le]), le);
    }

    public FlagData extraFind(KeyofBTree key, int le) {
        return extraHash[le - 1].get(key);
    }

    public int getMergeLength(int i) {
        return mergeList[i].size();
    }

    public int getMergePlace(int le, int index) {
        return mergeList[le].get(index).getKey();
    }

    public void mergeCreate() {
        mergeIter = new Iterator[layer];
        extraMergeIter = new Iterator[layer - 1];
        mergeList = new List[layer];
        //        sortMap = new HashMap[level];
        sortList = new List[layer];
        mergeIter[0] = hash[0].entrySet().iterator();
        mergeList[0] = new ArrayList<Entry<Integer, FlagData>>();
        //        sortMap[0] = new HashMap<NestCombKey, FlagData>();
        sortList[0] = new ArrayList<Entry<NestCombKey, FlagData>>();

        for (int i = 1; i < layer; i++) {
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
        sortIter = new Iterator[layer];
        for (int i = 0; i < layer; i++) {
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
        for (HashMap<KeyofBTree, FlagData> hh : hash)
            hh.clear();
        for (HashMap<KeyofBTree, FlagData> hh : extraHash)
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

    public Entry<KeyofBTree, FlagData> next(int le) {
        return mergeIter[le].next();
    }

    public Entry<KeyofBTree, FlagData> extraNext(int le) {
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
    public NestCombKey findSort(KeyofBTree key, int le) {
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

        public long getBytesSize() {
            long res = 5;
            if (data != null)
                res += data.toString().length();
            return res;
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

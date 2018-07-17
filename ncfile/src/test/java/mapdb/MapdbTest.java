package mapdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.mapdb.BTreeKeySerializer.BasicKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class MapdbTest {
    public static class TT {
        private BTreeMap[] tree;
        private int len;
        private List<Entry<Long, Integer>> tt;
        private int[] in;
        private int st;

        public TT(BTreeMap[] tree) {
            this.tree = tree;
            len = tree.length;
            tt = new ArrayList<Entry<Long, Integer>>();
            st = 0;
            create();
        }

        public void create() {
            in = new int[len];
            for (int i = 0; i < len; i++) {
                tt.add(tree[i].pollFirstEntry());
                in[i] = i;
            }

            for (int i = 0; i < (len - 1); i++) {
                for (int j = (i + 1); j < len; j++) {
                    if (tt.get(in[j]).getKey() < tt.get(in[i]).getKey()) {
                        int tm = in[i];
                        in[i] = in[j];
                        in[j] = tm;
                    }
                }
            }
        }

        public Entry<Long, Integer> next() {
            if (st < len) {
                Entry<Long, Integer> re = tt.get(in[st]);
                tt.set(in[st], tree[in[st]].pollFirstEntry());
                if (tt.get(in[st]) == null) {
                    st++;
                } else {
                    int m = st;
                    long k = tt.get(in[st]).getKey();
                    for (int i = (st + 1); i < len; i++) {
                        if (k > tt.get(in[i]).getKey()) {
                            m++;
                        } else {
                            break;
                        }
                    }
                    if (m > st) {
                        int tm = in[st];
                        for (int i = st; i < m; i++) {
                            in[i] = in[i + 1];
                        }
                        in[m] = tm;
                    }
                }
                return re;
            } else {
                return null;
            }
        }

        public boolean hasNext() {
            return (st < len);
        }
    }

    public static List<Long> getKeys(File file) throws IOException {
        List<Long> res = new ArrayList<Long>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|", 5);
            res.add(combine(Integer.parseInt(tmp[0]), Integer.parseInt(tmp[3])));
        }
        reader.close();
        return res;
    }

    public static long combine(int k1, int k2) {
        long res = k1;
        res <<= 32;
        res += k2;
        return res;
    }

    public static void test1(List<Long> keys, File file1, File file2, int max) throws IOException {
        DB db1 = DBMaker.newFileDB(file1).make();
        BTreeMap<Long, Integer> tree1 = db1.createTreeMap("tree").keySerializer(new BasicKeySerializer(Serializer.LONG))
                .valueSerializer(Serializer.INTEGER).make();
        int i = 0;
        long start = System.currentTimeMillis();
        for (Long key : keys) {
            tree1.put(key, i++);
            if ((i % max) == 0) {
                db1.commit();
            }
        }
        db1.commit();
        long end = System.currentTimeMillis();
        System.out.println("shuffle insert time: " + (end - start));
        db1.close();

        DB db2 = DBMaker.newFileDB(file2).make();
        BTreeMap<Long, Integer> tree2 = db2.createTreeMap("tree").keySerializer(new BasicKeySerializer(Serializer.LONG))
                .valueSerializer(Serializer.INTEGER).make();
        i = 0;
        int m = keys.size() / max;
        start = System.currentTimeMillis();
        for (int j = 0; j < m; j++) {
            List<Long> ks = keys.subList((j * max), (j * max + max - 1));
            Collections.sort(ks);
            for (Long key : ks) {
                tree2.put(key, i++);
            }
            db2.commit();
        }
        List<Long> ks = keys.subList((m * max), (keys.size() - 1));
        for (Long key : ks) {
            tree2.put(key, i++);
        }
        db2.commit();
        end = System.currentTimeMillis();
        System.out.println("sort insert time: " + (end - start));
        db2.close();
    }

    public static void test2(List<Long> keys, String path, int max) throws IOException {
        File p = new File(path);
        if (!p.exists()) {
            p.mkdirs();
        }
        long start = System.currentTimeMillis();
        DB db = DBMaker.newFileDB(new File(path + "result.db")).make();
        int i = 0;
        int m = keys.size() / max;
        BTreeMap<Long, Integer>[] tree = new BTreeMap[m + 1];
        for (int j = 0; j < m; j++) {
            tree[j] = db.createTreeMap("tree" + j).keySerializer(new BasicKeySerializer(Serializer.LONG))
                    .valueSerializer(Serializer.INTEGER).make();
            List<Long> ks = keys.subList((j * max), (j * max + max - 1));
            for (Long key : ks) {
                tree[j].put(key, i++);
            }
            db.commit();
        }
        List<Long> ks = keys.subList((m * max), (keys.size() - 1));
        tree[m] = db.createTreeMap("tree" + m).keySerializer(new BasicKeySerializer(Serializer.LONG))
                .valueSerializer(Serializer.INTEGER).make();
        for (Long key : ks) {
            tree[m].put(key, i++);
        }
        db.commit();
        TT read = new TT(tree);
        BTreeMap<Long, Integer> rtree = db.createTreeMap("rtree").keySerializer(new BasicKeySerializer(Serializer.LONG))
                .valueSerializer(Serializer.INTEGER).make();
        i = 0;
        while (read.hasNext()) {
            Entry<Long, Integer> rr = read.next();
            rtree.put(rr.getKey(), rr.getValue());
            i++;
            if ((i % max) == 0) {
                db.commit();
            }
        }
        db.commit();
        for (int j = 0; j < (m + 1); j++) {
            db.delete("tree" + j);
        }
        long end = System.currentTimeMillis();
        System.out.println("merge insert time: " + (end - start));
    }

    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        File dbFile1 = new File(args[1]);
        File dbFile2 = new File(args[2]);
        String path = args[3];
        int max = Integer.parseInt(args[4]);
        System.out.println("test: max=" + max);
        List<Long> keys = getKeys(file);
        Collections.shuffle(keys);

        test1(keys, dbFile1, dbFile2, max);
        test2(keys, path, max);
    }
}

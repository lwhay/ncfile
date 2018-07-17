package mapdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.mapdb.BTreeKeySerializer.BasicKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

//import api.ClusterConf;
//
public class BTreeTest {
    public static void btreeInsert(File dbFile, List<Integer> list, long max, int cachesize) throws IOException {
        long start = System.currentTimeMillis();
        System.out.println("###btree insert time###");
        DB db = DBMaker.newFileDB(dbFile).cacheSize(cachesize).make();
        BTreeMap<Integer, Integer> tree = db.createTreeMap("ok_btree")
                .keySerializer(new BasicKeySerializer(Serializer.INTEGER)).valueSerializer(Serializer.INTEGER).make();
        //BTreeMap tree = db.createTreeMap("ok_btree").keySerializer(new BasicKeySerializer(Serializer.LONG)).valueSerializer(Serializer.STRING).make();
        long k = 0;
        //    BufferedReader reader = new BufferedReader(new FileReader(file));
        //    String line;
        //    while((line = reader.readLine()) != null){
        //      k++;
        //      String[] tmp = line.split("\\|", 3);
        //      Object ok = Long.parseLong(tmp[0]);
        //      long ck = Long.parseLong(tmp[1]);
        //      tree.put(Fun.t2(ck, ok), tmp[2]);
        //      if((k % max) == 0){
        //        db.commit();
        //        long tt = System.currentTimeMillis();
        //        System.out.println((tt - start));
        //      }
        //    }
        //    reader.close();
        for (int v : list) {
            k++;
            tree.put(v, v);
            if ((k % max) == 0) {
                db.commit();//提交持久�?
                //        long tmp = System.currentTimeMillis();
                //        System.out.println((tmp - start));
                //        start = tmp;
            }
        }
        db.commit();
        long end = System.currentTimeMillis();
        System.out.println((end - start));
        db.close();
    }

    public static List<Integer> randomSort(File file) throws IOException {
        List<Integer> list = new ArrayList<Integer>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] tmp = line.split("\\|", 2);
            //            list.add((long) ((Integer.parseInt(tmp[0]) << 8) & (Integer.parseInt(tmp[3]))));
            list.add(Integer.parseInt(tmp[0]));
        }
        reader.close();
        //        Collections.shuffle(list);
        return list;
    }

    public static void btreeQuery(File dbFile, List<Integer> list, int cachesize) throws IOException {
        long start = System.currentTimeMillis();
        DB db = DBMaker.newFileDB(dbFile).cacheSize(cachesize).make();
        BTreeMap tree = db.getTreeMap("ok_btree");
        int t = 0, f = 0;
        //    BufferedReader reader = new BufferedReader(new FileReader(file));
        //    String line;
        //    while((line = reader.readLine()) != null){
        //      String[] tmp = line.split("\\|", 2);
        //      long ok = Long.parseLong(tmp[0]);
        //      //long ck = Long.parseLong(tmp[1]);
        //      if((tree.get(ok)) == null){
        //        f++;
        //      }else{
        //        t++;
        //      }
        //    }
        //    reader.close();
        for (int k : list) {
            if ((tree.get(k)) == null) {
                f++;
            } else {
                t++;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("$$exist: " + t + "\t$$not exist: " + f);
        System.out.println("##query time: " + (end - start));
        db.close();
    }

    public static void main(String[] args) throws IOException {
        //        List<Integer> list = randomSort(file);
        File file = new File(args[0]);
        File btreeFile = new File(args[1]);
        List<Integer> list = randomSort(file);
        int max = Integer.parseInt(args[2]);
        int cachesize = Integer.parseInt(args[3]);
        btreeInsert(btreeFile, list, max, cachesize);
    }
}
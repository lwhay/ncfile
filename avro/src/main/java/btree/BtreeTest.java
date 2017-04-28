package btree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BtreeTest {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String path = "g:\\bplustree\\data.txt";// index position
        String src = "g:\\Ѹ������\\quotes_2008-08.txt";// stanford db data
        final int max = 100000; // insert times
        final int treecachesize = 500; // cachesize, unit is block
        final int datacachesize = 500;
        final int blocksize = 4 * 1024; // blocksize
        final int nodeNumOfBlock = 10; // the number of node in a block
        final float cachefac = 0.6f; // cache weed out factor, staying default is ok

        Btree<Long, String> myTree = new Btree<Long, String>(nodeNumOfBlock, path, treecachesize, blocksize,
                datacachesize, cachefac);
        long count = 0;// read data
        long start = System.currentTimeMillis();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(src));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] temp = line.split(" ");
                myTree.insert(count, new Info(temp[0], temp[1]).content);
                count++;
                if (count == max) {
                    break;
                }
            }
            reader.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        System.out.println("insert time: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        System.out.println(myTree.find(8000L));
        for (long i = 0; i < max; i++) {
            myTree.find(i);
        }
        System.out.println("ordered find time: " + (System.currentTimeMillis() - start));
        System.out.println("success");
        myTree.close();
    }
}

package btree;

public class BtreeClusterTest {

    public static void main(String[] args) {
        String path = "g:\\bplustree\\data.txt";
        final int max = 10000000;
        BtreeCluster<Integer, Integer> myTree = new BtreeCluster<Integer, Integer>(50, path, 1000, 65536, 0.7f);
        long start = System.currentTimeMillis();
        for (int i = 0; i < max; i++) {
            myTree.insert(i, i);
        }
        System.out.println("keyNum " + myTree.getKeyNum());
        System.out.println("height " + myTree.getHeight());
        System.out.println("insert time: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        // System.out.println(myTree.find(5L));

        for (int i = 0; i < max; i++) {
            if (i != myTree.find(i)) {
                System.out.println("error " + i);
            }
        }
        System.out.println("find time: " + (System.currentTimeMillis() - start));

        System.out.println("Success");
        //myTree.close();
    }
}

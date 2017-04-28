package btree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/*
 * * this class is designed for Btree that data and leafnode are stored together.
 * It supports fixed length key and value.
 * It directly support Integer and Long as its key and value.
 * If you want to use self-defined type for key or value, these rules should be
 * followed:
 *     1.key must be fixed-length and implements java.lang.Comparable and serialize.iSerializable interface.
 *     2.value must be fixed-length and implements serialize.iSerializable interface.
 *
 * To use this Btree, first you should configure necessary parameters in api.ClusterConf and use this object
 *     to Construct this class, then you can use insert and find to operate, now update not supported.
 * Detailed use tips can be found in src/test/java.btree.UnClusteredBtreeTest.
 */

public class BtreeCluster<T extends Comparable<T>, V> {

    private Node root = null;
    private Class<?> keytype, valuetype;
    private TreeCache cache;
    private int nodeNumOfBlock, cacheSize, inodensize = 12, internsize, leafnsize, interInitial = -1, leafInitial = -2,
            keylen, valuelen, rootaddr, keyNum, blockSize, headaddr, tailaddr, size;
    private float cachefac;
    private RandomAccessFile raf;
    private byte[] nodeFlash, keyFlash, valueFlash;
    private boolean type;
    private String path;

    public BtreeCluster(int nodeNumOfBlock, String path, int cacheSize, int blocksize, float cachefac) {
        this.blockSize = blocksize;
        this.nodeNumOfBlock = nodeNumOfBlock;
        this.cacheSize = cacheSize;
        this.cachefac = cachefac;
        this.type = false;
        this.path = path.substring(0, path.lastIndexOf(File.separator) + 1)
                + path.substring(path.lastIndexOf(File.separator) + 1) + ".meta.data";
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        } else {
            if (!file.getParentFile().isDirectory()) {
                file.getParentFile().mkdirs();
            }
        }
        try {
            raf = new RandomAccessFile(path, "rw");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public BtreeCluster(String path) {
        this(20, path, 100, 65536, 0.6f);
    }

    public BtreeCluster() {
    }

    public void insert(T key, V value) {
        if (key == null)
            throw new NullPointerException("must not be null for key.");
        size++;
        if (!type) {
            keytype = key.getClass();
            valuetype = value.getClass();
            type = true;
            if (keytype == Integer.class) {
                keylen = 4;
            } else if (keytype == Long.class) {
                keylen = 8;
            } else if (keytype == Double.class) {
                keylen = 8;
            } else {
                keylen = ((Serializable) key).serialize().length;
            }
            if (valuetype == Integer.class) {
                valuelen = 4;
            } else if (valuetype == Long.class) {
                valuelen = 8;
            } else if (valuetype == Double.class) {
                valuelen = 8;
            } else {
                valuelen = ((Serializable) value).serialize().length;
            }
            if (valuelen > 4) {
                keyNum = ((blockSize - 1) / nodeNumOfBlock - inodensize - 8) / (keylen + valuelen) + 1;
            } else {
                keyNum = ((blockSize - 1) / nodeNumOfBlock - inodensize - 8) / (keylen + 4) - 1;
            }
            if (keyNum < 1) {
                keyNum = 1;
            }
            internsize = 4 * (keyNum + 1) + inodensize + (keyNum) * keylen;
            leafnsize = (keyNum - 1) * (keylen + valuelen) + inodensize + 8;
            nodeFlash = new byte[internsize > leafnsize ? internsize : leafnsize];
            keyFlash = new byte[keylen];
            valueFlash = new byte[valuelen];
            cache = new TreeCache(cacheSize, nodeNumOfBlock, raf, cachefac);
            root = cache.getNode(leafInitial);
            rootaddr = headaddr = tailaddr = root.addr;
        }
        Node node = root.insert(key, value, cache);
        if (node != null) {
            root = node;
            rootaddr = root.addr;
        }
    }

    public V find(T key) {
        return root.find(key, cache);
    }

    public void close() {
        cache.close();
        ByteArrayOutputStream byt = new ByteArrayOutputStream();
        try {
            ObjectOutputStream obj = new ObjectOutputStream(byt);
            obj.writeObject(keytype);
            obj.writeObject(valuetype);
            obj.writeInt(keylen);
            obj.writeInt(valuelen);
            obj.writeInt(keyNum);
            obj.writeInt(blockSize);
            obj.writeInt(cacheSize);
            obj.writeFloat(cachefac);
            obj.writeInt(nodeNumOfBlock);
            obj.writeInt(headaddr);
            obj.writeInt(tailaddr);
            obj.writeInt(size);
            obj.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        byte[] data = byt.toByteArray();
        try {
            RandomAccessFile metaraf = new RandomAccessFile(path, "rw");
            boolean typeOfRoot = root instanceof BtreeCluster.LeafNode;
            root.serialize();
            int length = typeOfRoot ? leafnsize : internsize;
            metaraf.writeInt(data.length);
            metaraf.write(data);
            metaraf.writeBoolean(typeOfRoot ? true : false);
            metaraf.writeInt(length);
            metaraf.write(nodeFlash, 0, length);
            metaraf.close();
            raf.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        root = null;

    }

    public void readMeta(String path) {
        try {
            this.path = path.substring(0, path.lastIndexOf(File.separator) + 1)
                    + path.substring(path.lastIndexOf(File.separator) + 1) + ".meta.data";
            RandomAccessFile raf = new RandomAccessFile(this.path, "r");
            int length = raf.readInt();
            byte[] data = new byte[length];
            raf.read(data);
            ByteArrayInputStream byteInput = new ByteArrayInputStream(data);
            ObjectInputStream objInput = new ObjectInputStream(byteInput);
            try {
                keytype = (Class<?>) objInput.readObject();
                valuetype = (Class<?>) objInput.readObject();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            keylen = objInput.readInt();
            valuelen = objInput.readInt();
            keyNum = objInput.readInt();
            blockSize = objInput.readInt();
            cacheSize = objInput.readInt();
            cachefac = objInput.readFloat();
            nodeNumOfBlock = objInput.readInt();
            headaddr = objInput.readInt();
            tailaddr = objInput.readInt();
            size = objInput.readInt();
            boolean type = raf.readBoolean();
            internsize = 4 * (keyNum + 1) + inodensize + (keyNum) * keylen;
            leafnsize = (keyNum - 1) * (keylen + valuelen) + inodensize + 8;
            nodeFlash = new byte[internsize > leafnsize ? internsize : leafnsize];
            keyFlash = new byte[keylen];
            valueFlash = new byte[valuelen];
            raf.read(nodeFlash, 0, raf.readInt());
            root = type ? new LeafNode() : new InternalNode();
            root.deserialize(nodeFlash);
            raf.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        type = true;
        try {
            raf = new RandomAccessFile(path, "rw");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void open(String path) {
        readMeta(path);
        cache = new TreeCache(cacheSize, nodeNumOfBlock, raf, cachefac);
        rootaddr = root.addr;
    }

    public void open(String path, int cacheSize) {
        readMeta(path);
        this.cacheSize = cacheSize;
        cache = new TreeCache(cacheSize, nodeNumOfBlock, raf, cachefac);
        rootaddr = root.addr;
    }

    public Iterator<Entry<T, V>> iterator() {
        return new InnerIterator();
    }

    public Iterator<Entry<T, V>> reverseIterator() {
        return new InnerReverseIterator();
    }

    @SuppressWarnings("unchecked")
    private class InnerIterator implements Iterator<Entry<T, V>> {
        private LeafNode curNode;
        private int curIndex;

        public InnerIterator() {
            curNode = (LeafNode) cache.getNode(headaddr);
            curIndex = 0;
        }

        @Override
        public boolean hasNext() {
            // TODO Auto-generated method stub
            if (curIndex >= curNode.size) {
                return false;
            }
            return true;
        }

        @Override
        public Entry<T, V> next() {
            // TODO Auto-generated method stub
            T key = (T) curNode.keys[curIndex];
            V value = (V) curNode.values[curIndex];
            curIndex++;
            if (curIndex >= curNode.size && curNode.next > 0) {
                curNode = (LeafNode) cache.getNode(curNode.next);
                curIndex = 0;
            }
            return new Entry<T, V>(key, value);
        }

    }

    @SuppressWarnings("unchecked")
    private class InnerReverseIterator implements Iterator<Entry<T, V>> {
        private LeafNode curNode;
        private int curIndex;

        public InnerReverseIterator() {
            curNode = (LeafNode) cache.getNode(tailaddr);
            curIndex = curNode.size - 1;
        }

        @Override
        public boolean hasNext() {
            // TODO Auto-generated method stub
            if (curIndex < 0) {
                return false;
            }
            return true;
        }

        @Override
        public Entry<T, V> next() {
            // TODO Auto-generated method stub
            T key = (T) curNode.keys[curIndex];
            V value = (V) curNode.values[curIndex];
            curIndex--;
            if (curIndex < 0 && curNode.pre >= 0) {
                curNode = (LeafNode) cache.getNode(curNode.pre);
                curIndex = curNode.size - 1;
            }
            return new Entry<T, V>(key, value);
        }

    }

    @SuppressWarnings("unchecked")
    public int getHeight() {
        int height = 1;
        Node node = root;
        while (!(node instanceof BtreeCluster.LeafNode)) {
            height++;
            node = cache.getNode(((InternalNode) node).pointers[0]);
        }
        return height;
    }

    public void printTree() {
        printNode(root);
    }

    public int getKeyNum() {
        return keyNum;
    }

    public String getPath() {
        return path;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    @SuppressWarnings("unchecked")
    private void printNode(Node node) {
        if (node == null) {
            return;
        }
        if (node instanceof BtreeCluster.LeafNode) {
            node.printInfo();
            return;
        }
        node.printInfo();
        for (int i = 0; i < node.size + 1; i++) {
            printNode(cache.getNode(((InternalNode) node).pointers[i]));
        }
    }

    public abstract class Node {
        public int parent;
        public Object[] keys;
        public int size;
        public int addr;
        private boolean dirty;
        private boolean isUsing;

        public abstract Node insert(T key, V value, TreeCache cache);

        //
        public abstract V find(T key, TreeCache cache);

        public void serialize() {
            int pos = 0;
            nodeFlash[pos++] = (byte) (parent & 0xff);
            nodeFlash[pos++] = (byte) ((parent & 0xff00) >> 8);
            nodeFlash[pos++] = (byte) ((parent & 0xff0000) >> 16);
            nodeFlash[pos++] = (byte) ((parent & 0xff000000) >> 24);
            nodeFlash[pos++] = (byte) (size & 0xff);
            nodeFlash[pos++] = (byte) ((size & 0xff00) >> 8);
            nodeFlash[pos++] = (byte) ((size & 0xff0000) >> 16);
            nodeFlash[pos++] = (byte) ((size & 0xff000000) >> 24);
            nodeFlash[pos++] = (byte) (addr & 0xff);
            nodeFlash[pos++] = (byte) ((addr & 0xff00) >> 8);
            nodeFlash[pos++] = (byte) ((addr & 0xff0000) >> 16);
            nodeFlash[pos++] = (byte) ((addr & 0xff000000) >> 24);
        }

        public void deserialize(byte[] data) {
            int pos = 0;
            parent = Utils.getInt(data, pos);
            pos += 4;
            size = Utils.getInt(data, pos);
            pos += 4;
            addr = Utils.getInt(data, pos);
            pos += 4;
        }

        public boolean isChanged() {
            return dirty;
        }

        public void setChanged(boolean dirty) {
            this.dirty = dirty;
        }

        public boolean isUsing() {
            return isUsing;
        }

        public void setUsing(boolean isUsing) {
            this.isUsing = isUsing;
        }

        public abstract void printInfo();
    }

    class InternalNode extends Node {
        public int[] pointers;

        public InternalNode() {
            this.size = 0;
            this.pointers = new int[keyNum + 1];
            this.setChanged(true);
            this.parent = interInitial;
            this.keys = new Object[keyNum];
        }

        @SuppressWarnings("unchecked")
        public Node insert(T key, V value, TreeCache cache) {
            int pos = -1;
            if (this.size == 0 || key.compareTo((T) this.keys[0]) < 0) {
                pos = 0;
            } else if (key.compareTo((T) this.keys[this.size - 1]) > 0) {
                pos = this.size;
            } else {
                pos = Utils.bSearch(this.keys, 0, this.size, key);
                if (this.keys[pos] == key) {
                    pos++;
                }
            }
            Node child = cache.getNode(pointers[pos]);
            Node node = child.insert(key, value, cache);
            return node;
        }

        //
        @SuppressWarnings("unchecked")
        Node insert(T key, int leftChild, int rightChild, TreeCache cache) {
            this.setChanged(true);
            this.setUsing(true);
            if (this.size == 0) {
                this.size++;
                this.pointers[0] = leftChild;
                this.pointers[1] = rightChild;
                this.keys[0] = key;
                this.setUsing(false);
                return this;
            }
            Object[] newKeys = new Object[keyNum + 1];
            int[] newPointers = new int[keyNum + 2];
            int pos = -1;
            if (this.size == 0 || key.compareTo((T) this.keys[0]) < 0) {
                pos = 0;
            } else if (key.compareTo((T) this.keys[this.size - 1]) > 0) {
                pos = this.size;
            } else {
                pos = Utils.bSearch(this.keys, 0, this.size, key);
                if (((T) this.keys[pos]).compareTo(key) == 0) {
                    pos++;
                }
            }
            System.arraycopy(this.keys, 0, newKeys, 0, pos);
            newKeys[pos] = key;
            System.arraycopy(this.keys, pos, newKeys, pos + 1, this.size - pos);
            System.arraycopy(this.pointers, 0, newPointers, 0, pos + 1);
            newPointers[pos + 1] = rightChild;
            System.arraycopy(this.pointers, pos + 1, newPointers, pos + 2, this.size - pos);
            this.size++;
            if (this.size <= keyNum) {
                System.arraycopy(newKeys, 0, this.keys, 0, this.size);
                System.arraycopy(newPointers, 0, this.pointers, 0, this.size + 1);
                this.setUsing(false);
                return null;
            }
            int m = (this.size / 2);
            int total = this.size;
            System.arraycopy(newKeys, 0, this.keys, 0, m);
            System.arraycopy(newPointers, 0, this.pointers, 0, m + 1);
            this.size = m;
            InternalNode newNode = (InternalNode) cache.getNode(interInitial);
            newNode.setUsing(true);
            newNode.size = total - m - 1;
            System.arraycopy(newKeys, m + 1, newNode.keys, 0, total - m - 1);
            System.arraycopy(newPointers, m + 1, newNode.pointers, 0, total - m);
            for (int j = 0; j < newNode.size + 1; j++) {
                Node child = cache.getNode(newNode.pointers[j]);
                child.parent = newNode.addr;
                child.setChanged(true);
            }
            InternalNode parentNode = (InternalNode) cache.getNode(this.parent);
            newNode.parent = this.parent = parentNode.addr;
            newNode.setUsing(false);
            Node node = (parentNode).insert((T) newKeys[m], this.addr, newNode.addr, cache);
            this.setUsing(false);
            return node;
        }

        @SuppressWarnings("unchecked")
        public V find(T key, TreeCache cache) {
            int i = 0;
            for (; i < this.size; i++) {
                if (key.compareTo((T) this.keys[i]) < 0)
                    break;
            }
            Node child = cache.getNode(this.pointers[i]);
            return child.find(key, cache);
        }

        public void serialize() {
            super.serialize();
            int pos = inodensize;
            if (keytype == Integer.class) {
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] == null) {
                        for (int j = 0; j < keylen; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (keys.length - i - 1) * keylen;
                        break;
                    }
                    Utils.getBytes4((int) keys[i], nodeFlash, pos);
                    pos += keylen;
                }
            } else if (keytype == Long.class) {
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] == null) {
                        for (int j = 0; j < keylen; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (keys.length - i - 1) * keylen;
                        break;
                    }
                    Utils.getBytes8((long) keys[i], nodeFlash, pos);
                    pos += keylen;
                }
            } else if (keytype == Double.class) {
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] == null) {
                        for (int j = 0; j < keylen; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (keys.length - i - 1) * keylen;
                        break;
                    }
                    Utils.getDoubleBytes((double) keys[i], nodeFlash, pos);
                    pos += keylen;
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] == null) {
                        for (int j = 0; j < keylen; j++) {
                            nodeFlash[pos++] = -1;
                        }
                        pos += (keys.length - i - 1) * keylen;
                        break;
                    }
                    byte[] temp = ((Serializable) keys[i]).serialize();
                    System.arraycopy(temp, 0, nodeFlash, pos, temp.length);
                    pos += temp.length;

                }
            }
            for (int i = 0; i < pointers.length; i++) {
                nodeFlash[pos++] = (byte) (pointers[i] & 0xff);
                nodeFlash[pos++] = (byte) ((pointers[i] & 0xff00) >> 8);
                nodeFlash[pos++] = (byte) ((pointers[i] & 0xff0000) >> 16);
                nodeFlash[pos++] = (byte) ((pointers[i] & 0xff000000) >> 24);
            }
        }

        public void deserialize(byte[] data) {
            super.deserialize(data);
            int pos = inodensize;
            if (keytype == Integer.class) {
                for (int i = 0; i < keys.length; i++) {
                    int j = 0;
                    for (; j < keylen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == keylen) {
                        pos += (keys.length - i) * keylen;
                        break;
                    }
                    keys[i] = Utils.getInt(data, pos);
                    pos += keylen;
                }
            } else if (keytype == Long.class) {
                for (int i = 0; i < keys.length; i++) {
                    int j = 0;
                    for (; j < keylen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == keylen) {
                        pos += (keys.length - i) * keylen;
                        break;
                    }
                    keys[i] = Utils.getLong(data, pos);
                    pos += keylen;
                }
            } else if (keytype == Double.class) {
                for (int i = 0; i < keys.length; i++) {
                    int j = 0;
                    for (; j < keylen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == keylen) {
                        pos += (keys.length - i) * keylen;
                        break;
                    }
                    keys[i] = Utils.getDouble(data, pos);
                    pos += keylen;
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    int j = 0;
                    for (; j < keylen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == keylen) {
                        pos += (keys.length - i) * keylen;
                        break;
                    }
                    System.arraycopy(data, pos, keyFlash, 0, keylen);
                    try {
                        keys[i] = (Serializable) keytype.newInstance();
                    } catch (InstantiationException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    ((Serializable) keys[i]).deseriablize(keyFlash);
                    pos += keylen;
                }
            }
            for (int i = 0; i < pointers.length; i++) {
                pointers[i] = Utils.getInt(data, pos);
                pos += 4;
            }
        }

        public void printInfo() {
            int pointerlen;
            System.out.println("this.size = " + this.size);
            System.out.println("this.addr = " + this.addr);
            System.out.println("this.parent = " + this.parent);
            pointerlen = this.size + 1;
            System.out.println("InternalNode ");
            for (int i = 0; i < this.size; i++) {
                System.out.print(i + ":" + keys[i] + " ");
            }
            System.out.print(" : ");
            for (int i = 0; i < pointerlen; i++) {
                System.out.print(i + ":" + pointers[i] + " ");
            }
            System.out.println();
        }
    }

    private class LeafNode extends Node {
        Object[] values;
        int pre, next;

        public LeafNode() {
            this.size = 0;
            this.pre = this.next = -1;
            this.values = new Object[keyNum - 1];
            this.setChanged(true);
            this.parent = interInitial;
            this.keys = new Object[keyNum - 1];
        }

        @SuppressWarnings("unchecked")
        public Node insert(T key, V value, TreeCache cache) {
            this.setChanged(true);
            this.setUsing(true);
            Object[] newKeys = new Object[keyNum];
            Object[] newPointers = new Object[keyNum];
            int pos = -1;
            if (this.size == 0 || key.compareTo((T) this.keys[0]) < 0) {
                pos = 0;
            } else if (key.compareTo((T) this.keys[this.size - 1]) > 0) {
                pos = this.size;
            } else {
                pos = Utils.bSearch(this.keys, 0, this.size, key);
                if (((T) this.keys[pos]).compareTo(key) == 0) {
                    this.values[pos] = value;
                    this.setUsing(false);
                    return null;
                }
            }
            System.arraycopy(this.keys, 0, newKeys, 0, pos);
            newKeys[pos] = key;
            System.arraycopy(this.keys, pos, newKeys, pos + 1, this.size - pos);
            System.arraycopy(this.values, 0, newPointers, 0, pos);
            newPointers[pos] = value;
            System.arraycopy(this.values, pos, newPointers, pos + 1, this.size - pos);
            this.size++;
            if (this.size <= keyNum - 1) {
                System.arraycopy(newKeys, 0, this.keys, 0, this.size);
                System.arraycopy(newPointers, 0, this.values, 0, this.size);
                this.setUsing(false);
                return null;
            }
            int m = this.size / 2;
            int total = this.size;
            System.arraycopy(newKeys, 0, this.keys, 0, m);
            System.arraycopy(newPointers, 0, this.values, 0, m);
            this.size = m;
            LeafNode newNode = (LeafNode) cache.getNode(leafInitial);
            newNode.setUsing(true);
            newNode.size = total - m;
            newNode.next = this.next;
            newNode.pre = this.addr;
            if (this.next != -1) {
                LeafNode temp = (LeafNode) cache.getNode(this.next);
                temp.pre = newNode.addr;
                temp.setChanged(true);
            }
            this.next = newNode.addr;
            if (this.addr == tailaddr) {
                tailaddr = newNode.addr;
            }
            System.arraycopy(newKeys, m, newNode.keys, 0, total - m);
            System.arraycopy(newPointers, m, newNode.values, 0, total - m);
            InternalNode parentNode = (InternalNode) cache.getNode(this.parent);
            newNode.parent = this.parent = parentNode.addr;
            newNode.setUsing(false);
            Node node = (parentNode).insert((T) newNode.keys[0], this.addr, newNode.addr, cache);
            this.setUsing(false);
            return node;
        }

        //
        @SuppressWarnings("unchecked")
        public V find(T key, TreeCache cache) {
            if (this.size == 0) {
                return null;
            }
            int middle = Utils.bSearch(this.keys, 0, this.size, key);
            if (middle >= this.size) {
                return null;
            }
            T middleKey = (T) this.keys[middle];
            return middleKey.compareTo(key) == 0 ? (V) this.values[middle] : null;
        }

        public void serialize() {
            super.serialize();
            int pos = inodensize;
            nodeFlash[pos++] = (byte) (pre & 0xff);
            nodeFlash[pos++] = (byte) ((pre & 0xff00) >> 8);
            nodeFlash[pos++] = (byte) ((pre & 0xff0000) >> 16);
            nodeFlash[pos++] = (byte) ((pre & 0xff000000) >> 24);
            nodeFlash[pos++] = (byte) (next & 0xff);
            nodeFlash[pos++] = (byte) ((next & 0xff00) >> 8);
            nodeFlash[pos++] = (byte) ((next & 0xff0000) >> 16);
            nodeFlash[pos++] = (byte) ((next & 0xff000000) >> 24);
            if (keytype == Integer.class) {
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] == null) {
                        for (int j = 0; j < keylen; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (keys.length - i - 1) * keylen;
                        break;
                    }
                    Utils.getBytes4((int) keys[i], nodeFlash, pos);
                    pos += keylen;
                }
            } else if (keytype == Long.class) {
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] == null) {
                        for (int j = 0; j < 8; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (keys.length - i - 1) * 8;
                        break;
                    }
                    Utils.getBytes8((long) keys[i], nodeFlash, pos);
                    pos += 8;
                }
            } else if (keytype == Double.class) {
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] == null) {
                        for (int j = 0; j < 8; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (keys.length - i - 1) * 8;
                        break;
                    }
                    Utils.getDoubleBytes((double) keys[i], nodeFlash, pos);
                    pos += 8;
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    if (keys[i] == null) {
                        for (int j = 0; j < keylen; j++) {
                            nodeFlash[pos++] = -1;
                        }
                        pos += (keys.length - i - 1) * keylen;
                        break;
                    }
                    byte[] temp = ((Serializable) keys[i]).serialize();
                    System.arraycopy(temp, 0, nodeFlash, pos, temp.length);
                    pos += temp.length;

                }
            }
            if (valuetype == Integer.class) {
                for (int i = 0; i < values.length; i++) {
                    if (values[i] == null) {
                        for (int j = 0; j < valuelen; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (values.length - i - 1) * valuelen;
                        break;
                    }
                    Utils.getBytes4((int) values[i], nodeFlash, pos);
                    pos += valuelen;
                }
            } else if (valuetype == Long.class) {
                for (int i = 0; i < values.length; i++) {
                    if (values[i] == null) {
                        for (int j = 0; j < valuelen; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (values.length - i - 1) * valuelen;
                        break;
                    }
                    Utils.getBytes8((long) values[i], nodeFlash, pos);
                    pos += valuelen;
                }
            } else if (valuetype == Double.class) {
                for (int i = 0; i < values.length; i++) {
                    if (values[i] == null) {
                        for (int j = 0; j < valuelen; j++) {

                            nodeFlash[pos++] = -1;
                        }
                        pos += (values.length - i - 1) * valuelen;
                        break;
                    }
                    Utils.getDoubleBytes((double) values[i], nodeFlash, pos);
                    pos += valuelen;
                }
            } else {
                for (int i = 0; i < values.length; i++) {
                    if (values[i] == null) {
                        for (int j = 0; j < valuelen; j++) {
                            nodeFlash[pos++] = -1;
                        }
                        pos += (values.length - i - 1) * valuelen;
                        break;
                    }
                    byte[] temp = ((Serializable) values[i]).serialize();
                    System.arraycopy(temp, 0, nodeFlash, pos, temp.length);
                    pos += temp.length;

                }
            }
        }

        public void deserialize(byte[] data) {
            super.deserialize(data);
            int pos = inodensize;
            pre = Utils.getInt(data, pos);
            pos += 4;
            next = Utils.getInt(data, pos);
            pos += 4;
            if (keytype == Integer.class) {
                for (int i = 0; i < keys.length; i++) {
                    int j = 0;
                    for (; j < keylen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == keylen) {
                        pos += (keys.length - i) * keylen;
                        break;
                    }
                    keys[i] = Utils.getInt(data, pos);
                    pos += keylen;
                }
            } else if (keytype == Long.class) {
                for (int i = 0; i < keys.length; i++) {
                    int j = 0;
                    for (; j < keylen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == keylen) {
                        pos += (keys.length - i) * keylen;
                        break;
                    }
                    keys[i] = Utils.getLong(data, pos);
                    pos += keylen;
                }
            } else if (keytype == Double.class) {
                for (int i = 0; i < keys.length; i++) {
                    int j = 0;
                    for (; j < keylen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == keylen) {
                        pos += (keys.length - i) * keylen;
                        break;
                    }
                    keys[i] = Utils.getDouble(data, pos);
                    pos += keylen;
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    int j = 0;
                    for (; j < keylen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == keylen) {
                        pos += (keys.length - i) * keylen;
                        break;
                    }
                    System.arraycopy(data, pos, keyFlash, 0, keylen);
                    try {
                        keys[i] = (Serializable) keytype.newInstance();
                    } catch (InstantiationException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    ((Serializable) keys[i]).deseriablize(keyFlash);
                    pos += keylen;
                }
            }
            if (valuetype == Integer.class) {
                for (int i = 0; i < values.length; i++) {
                    int j = 0;
                    for (; j < valuelen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == valuelen) {
                        pos += (values.length - i) * valuelen;
                        break;
                    }
                    values[i] = Utils.getInt(data, pos);
                    pos += valuelen;
                }
            } else if (valuetype == Long.class) {
                for (int i = 0; i < values.length; i++) {
                    int j = 0;
                    for (; j < valuelen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == valuelen) {
                        pos += (values.length - i) * valuelen;
                        break;
                    }
                    values[i] = Utils.getLong(data, pos);
                    pos += valuelen;
                }
            } else if (valuetype == Double.class) {
                for (int i = 0; i < values.length; i++) {
                    int j = 0;
                    for (; j < valuelen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == valuelen) {
                        pos += (values.length - i) * valuelen;
                        break;
                    }
                    values[i] = Utils.getDouble(data, pos);
                    pos += valuelen;
                }
            } else {
                for (int i = 0; i < values.length; i++) {
                    int j = 0;
                    for (; j < valuelen; j++) {
                        if (data[pos + j] != -1) {
                            break;
                        }
                    }
                    if (j == valuelen) {
                        pos += (values.length - i) * valuelen;
                        break;
                    }
                    System.arraycopy(data, pos, valueFlash, 0, valuelen);

                    try {
                        values[i] = (Serializable) valuetype.newInstance();
                    } catch (InstantiationException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                    ((Serializable) values[i]).deseriablize(valueFlash);
                    pos += valuelen;
                }
            }
        }

        public void printInfo() {
            int pointerlen;
            System.out.println("this.size = " + this.size);
            System.out.println("this.addr = " + this.addr);
            System.out.println("this.parent = " + this.parent);
            pointerlen = this.size;
            System.out.print("LeafNode ");
            for (int i = 0; i < this.size; i++) {
                System.out.print(i + ":" + keys[i] + " ");
            }
            System.out.print(" : ");
            for (int i = 0; i < pointerlen; i++) {
                System.out.print(i + ":" + values[i] + " ");
            }
            System.out.println();
        }
    }

    class TreeCache {
        private final int capacity;
        private Pair first, last;
        private HashMap<Integer, Pair> hashMap;
        private int discard, interMax, leafMax, blockBase, leafno, leafoff, interno, interoff;
        private RandomAccessFile raf;
        private NodeCache nodemap;

        public TreeCache(int treecachesize, int nodeNumOfBlock, RandomAccessFile raf, float cachefac) {
            leafoff = interno = interoff = 0;
            leafno = 1;
            this.capacity = treecachesize;
            this.raf = raf;
            this.interMax = (blockSize - 1) / internsize;
            this.leafMax = (blockSize - 1) / leafnsize;
            this.blockBase = (int) Math.pow(2, ((Math.log(Math.max(interMax, leafMax)) / Math.log(2)) + 1));
            nodemap = new NodeCache(Math.max(100, keyNum * 3), blockBase, this, raf);
            this.discard = (int) (capacity * (1 - cachefac));
            hashMap = new HashMap<Integer, Pair>();
        }

        public Node getNode(int addr) {
            if (nodemap.containsKey(addr)) {
                Node node = nodemap.get(addr);
                return node;
            }
            if (addr == -1) {
                Node newnode = new InternalNode();
                addr = interno * blockBase + interoff;
                newnode.addr = addr;
                interoff++;
                if (interoff >= interMax) {
                    interoff = 0;
                    interno = interno < leafno ? leafno + 1 : interno + 1;
                }
                nodemap.put(addr, newnode);
                return newnode;
            } else if (addr == -2) {
                Node newnode = new LeafNode();
                addr = leafno * blockBase + leafoff;
                newnode.addr = addr;
                leafoff++;
                if (leafoff >= leafMax) {
                    leafoff = 0;
                    leafno = leafno < interno ? interno + 1 : leafno + 1;
                }
                nodemap.put(addr, newnode);
                return newnode;
            }
            int bno = addr / blockBase;
            int nno = addr % blockBase;
            if (this.containsKey(bno)) {
                Block block = this.get(bno);
                Node node = block.getNode(nno);
                node.setChanged(false);
                nodemap.put(addr, node);
                return node;
            }
            try {
                raf.seek((long) bno * blockSize);
                Block block = new Block(0);
                byte[] data = new byte[blockSize];
                raf.read(data);
                block.load(data);
                this.put(bno, block);
                Node node = block.getNode(nno);
                node.setChanged(false);
                nodemap.put(addr, node);
                return node;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return null;
        }

        public void put(Integer key, Block value) {
            Pair entry = getPair(key);
            if (entry == null) {
                if (hashMap.size() > capacity) {
                    Map<Integer, Block> temp = new TreeMap<Integer, Block>();
                    for (int i = 0; i < discard; i++) {
                        if (last.value.isChanged()) {
                            temp.put(last.key, last.value);
                        }
                        hashMap.remove(last.key);
                        removeLast();
                    }
                    for (Iterator<Integer> it = temp.keySet().iterator(); it.hasNext();) {
                        try {
                            int bno = it.next();
                            raf.seek((long) bno * blockSize);
                            raf.write(temp.get(bno).store());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                entry = new Pair();
                entry.key = key;
            }
            entry.value = value;
            moveToFirst(entry);
            hashMap.put(key, entry);
        }

        public void close() {
            nodemap.close();
            Map<Integer, Block> temp = new TreeMap<Integer, Block>();
            while (last != null) {
                if (last.value.isChanged()) {
                    temp.put(last.key, last.value);
                }
                hashMap.remove(last.key);
                removeLast();
            }
            for (Iterator<Integer> it = temp.keySet().iterator(); it.hasNext();) {
                try {
                    int bno = it.next();
                    raf.seek((long) bno * blockSize);
                    raf.write(temp.get(bno).store());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public Block get(Integer key) {
            Pair pair = getPair(key);
            if (pair == null)
                return null;
            moveToFirst(pair);
            return pair.value;
        }

        public boolean containsKey(Integer key) {
            if (hashMap.containsKey(key)) {
                return true;
            }
            return false;
        }

        private void moveToFirst(Pair entry) {
            if (entry == first)
                return;
            if (entry.pre != null)
                entry.pre.next = entry.next;
            if (entry.next != null)
                entry.next.pre = entry.pre;
            if (entry == last)
                last = last.pre;
            if (first == null || last == null) {
                first = last = entry;
                return;
            }
            entry.next = first;
            first.pre = entry;
            first = entry;
            entry.pre = null;
        }

        private void removeLast() {
            if (last != null) {
                last = last.pre;
                if (last == null)
                    first = null;
                else
                    last.next = null;
            }
        }

        private Pair getPair(Integer key) {
            return hashMap.get(key);
        }

        class Pair {
            public Pair pre;
            public Pair next;
            public Integer key;
            public Block value;
        }

        public int size() {
            return hashMap.size();
        }

    }

    public class NodeCache {
        private final int capacity;
        private Pair first, last;
        private int blockBase;
        private HashMap<Integer, Pair> hashMap;
        protected TreeCache cache;
        protected RandomAccessFile raf;

        public NodeCache(int size, int blockBase, TreeCache cache, RandomAccessFile raf) {
            this.cache = cache;
            this.capacity = size;
            this.raf = raf;
            this.blockBase = blockBase;
            hashMap = new HashMap<Integer, Pair>();
        }

        public void put(Integer key, Node value) {
            Pair pair = getPair(key);
            if (pair == null) {
                if (hashMap.size() > capacity) {
                    for (int i = 0; i < capacity * 0.4; i++) {
                        while (last.value.isUsing() || last.value.addr == rootaddr) {
                            moveToFirst(last);
                        }
                        if (last.value.isChanged()) {
                            int addr = last.key;
                            int bno = addr / blockBase;
                            int nno = addr % blockBase;
                            Block block = cache.get(bno);
                            if (block == null) {
                                try {
                                    raf.seek((long) bno * blockSize);
                                    byte[] temp = new byte[1];
                                    raf.read(temp);
                                    if (temp[0] == 0) {
                                        int type = last.value instanceof BtreeCluster.LeafNode ? 2 : 1;
                                        block = new Block(type);

                                    } else {
                                        block = new Block();
                                        byte[] data = new byte[blockSize];

                                        data[0] = temp[0];
                                        raf.read(data, 1, data.length - 1);
                                        block.load(data);
                                    }
                                } catch (IOException e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                                cache.put(bno, block);
                            }
                            block.setNode(last.value, nno);
                        }

                        hashMap.remove(last.key);
                        removeLast();
                    }

                }
                pair = new Pair();
                pair.key = key;
            }
            pair.value = value;
            moveToFirst(pair);
            hashMap.put(key, pair);
        }

        public void close() {
            while (last != null) {
                if (last.value.isChanged()) {
                    int addr = last.key;
                    int bno = addr / blockBase;
                    int nno = addr % blockBase;
                    Block block = cache.get(bno);
                    if (block == null) {
                        try {
                            raf.seek((long) bno * blockSize);
                            byte[] temp = new byte[1];
                            raf.read(temp);
                            if (temp[0] == 0) {
                                int type = last.value instanceof BtreeCluster.LeafNode ? 2 : 1;
                                block = new Block(type);
                            } else {
                                block = new Block();
                                byte[] data = new byte[blockSize];
                                data[0] = temp[0];
                                raf.read(data, 1, data.length - 1);
                                block.load(data);
                            }
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        cache.put(bno, block);
                    }
                    block.setNode(last.value, nno);
                }
                hashMap.remove(last.key);
                removeLast();
            }
        }

        public Node get(Integer key) {
            Pair entry = getPair(key);
            if (entry == null)
                return null;
            moveToFirst(entry);
            return entry.value;
        }

        public boolean containsKey(Integer key) {
            if (hashMap.containsKey(key)) {
                return true;
            }
            return false;
        }

        private void moveToFirst(Pair entry) {
            if (entry == first)
                return;
            if (entry.pre != null)
                entry.pre.next = entry.next;
            if (entry.next != null)
                entry.next.pre = entry.pre;
            if (entry == last)
                last = last.pre;
            if (first == null || last == null) {
                first = last = entry;
                return;
            }
            entry.next = first;
            first.pre = entry;
            first = entry;
            entry.pre = null;
        }

        private void removeLast() {
            if (last != null) {
                last = last.pre;
                if (last == null)
                    first = null;
                else
                    last.next = null;
            }
        }

        private Pair getPair(Integer key) {
            return hashMap.get(key);
        }

        class Pair {
            public Pair pre;
            public Pair next;
            public int key;
            public Node value;
        }

        public int size() {
            return hashMap.size();
        }
    }

    public class Block {
        protected byte[] data;
        private boolean dirty;

        public Block() {
            data = new byte[blockSize];
            this.dirty = false;
        }

        public Block(int type) {
            data = new byte[blockSize];
            this.dirty = true;
            if (type == 1) {
                data[0] = 1;
            } else if (type == 2) {
                data[0] = 2;
            }
        }

        public byte[] store() {
            return data;
        }

        public void load(byte[] data) {
            this.dirty = false;
            this.data = data;
        }

        public Node getNode(int offset) {
            Node node = null;
            int length = 0;
            if (data[0] == 1) {
                length = internsize;
                node = new InternalNode();
            } else if (data[0] == 2) {
                length = leafnsize;
                node = new LeafNode();
            }
            System.arraycopy(data, offset * length + 1, nodeFlash, 0, length);
            node.deserialize(nodeFlash);
            return node;
        }

        public void setNode(Node node, int offset) {
            node.serialize();
            int length = data[0] == 1 ? internsize : leafnsize;
            System.arraycopy(nodeFlash, 0, this.data, offset * length + 1, length);
            this.dirty = true;
        }

        public boolean isChanged() {
            return this.dirty;
        }
    }

    public static class Entry<T, V> {
        private T key;
        private V value;

        public Entry(T key, V value) {
            this.key = key;
            this.value = value;
        }

        public T getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "Entry [key=" + key + ", value=" + value + "]";
        }

    }

    public int getSize() {
        return size;
    }

}

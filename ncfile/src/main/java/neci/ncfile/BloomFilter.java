package neci.ncfile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import io.CacheBuffer;
import io.PageBuffer;
import neci.ncfile.base.Schema;
import neci.ncfile.generic.GenericData.Record;

public class BloomFilter {
    private int numHashes;
    private int numElements;
    private long numBits;
    private int numPages;
    private int numBitsPerPage;
    private final Schema schema;
    private final int[] keyFields;
    private File bloomFile;
    private CacheBuffer cache;
    private final static long SEED = 0L;

    private boolean isActivated = false;

    public BloomFilter(String filePath, Schema schema) throws IOException {
        this(filePath, schema, null);
    }

    public BloomFilter(String filePath, Schema schema, int[] keyFields) throws IOException {
        this.bloomFile = new File(filePath);
        this.schema = schema;
        this.keyFields = keyFields;
        this.numBitsPerPage = CacheBuffer.getPageSize() * 8;
    }

    public void activate() throws IOException {
        if (bloomFile.exists()) {
            this.cache = new CacheBuffer(bloomFile);
            this.numHashes = cache.getNumHashes();
            this.numElements = cache.getNumElements();
            this.numPages = cache.getNumPages();
            this.numBits = cache.getNumBits();
            isActivated = true;
        }
    }

    public void close() throws IOException {
        if (isActivated) {
            cache.close();
            isActivated = false;
        }
    }

    public void cover() throws IOException {
        close();
        if (bloomFile.exists())
            NestManager.shDelete(bloomFile.getAbsolutePath());
        //            bloomFile.delete();
    }

    public int getNumElements() {
        return numElements;
    }

    public boolean isActivated() {
        return isActivated;
    }

    public boolean contains(Record record, long[] hashes) throws IOException {
        return (contains(new KeyToBytes(record), hashes));
    }

    public boolean contains(Record record, boolean isKey, long[] hashes) throws IOException {
        return isKey ? contains(record, hashes) : contains(record, keyFields, hashes);
    }

    public boolean contains(Record record, int[] keyFields, long[] hashes) throws IOException {
        return (contains(new KeyToBytes(record, keyFields), hashes));
    }

    public boolean contains(KeyToBytes by, long[] hashes) throws IOException {
        if (numPages == 0) {
            return false;
        }
        if (!isActivated) {
            throw new IOException("The bloomfilter is not activated!");
        }
        HashTo128Bit.hashto128(by, SEED, hashes);
        for (int i = 0; i < numHashes; ++i) {
            long hash = Math.abs((hashes[0] + i * hashes[1]) % numBits);
            int pageNo = (int) (hash / numBitsPerPage);
            PageBuffer page = cache.getPage(pageNo);
            int byteNo = (int) (hash % numBitsPerPage) >> 3;
            byte b = page.read(byteNo);
            int bitNo = (int) (hash % numBitsPerPage) & 0x07;
            if (!((b & (1L << bitNo)) != 0)) {
                return false;
            }
        }
        return true;
    }

    public BloomFilterBuilder creatBuilder(int numElements, int numHashes, int numBitsPerElement) throws IOException {
        return new BloomFilterBuilder(numElements, numHashes, numBitsPerElement);
    }

    public class BloomFilterBuilder {
        private final long[] hashes = new long[2];
        private final int numElements;
        private final int numHashes;
        private final long numBits;
        private final int numPages;
        private PageBuffer[] pages;

        public BloomFilterBuilder(int numElements, int numHashes, int numBitsPerElement) throws IOException {
            this.numElements = numElements;
            this.numHashes = numHashes;
            numBits = this.numElements * numBitsPerElement;
            long tmp = (long) Math.ceil(numBits / (double) numBitsPerPage);
            if (tmp > Integer.MAX_VALUE) {
                throw new IOException("Cannot create a bloom filter with his huge number of pages.");
            }
            numPages = (int) tmp;
            pages = new PageBuffer[numPages];
            for (int i = 0; i < numPages; i++) {
                pages[i] = new PageBuffer();
            }
        }

        public void add(Record record, boolean isKey) throws IOException {
            KeyofBTree k = isKey ? new KeyofBTree(record, keyFields.length) : new KeyofBTree(record, keyFields);
            add(k);
        }

        public void add(KeyofBTree key) throws IOException {
            add(new KeyToBytes(key));
        }

        public void add(Record record) throws IOException {
            if (keyFields != null) {
                add(record, keyFields);
            } else {
                add(new KeyToBytes(record));
            }
        }

        public void add(Record record, int[] keyFields) throws IOException {
            add(new KeyToBytes(record, keyFields));
        }

        public void add(KeyToBytes by) throws IOException {
            if (numPages == 0) {
                throw new IOException(
                        "Cannot add elements to this filter since it is supposed to be empty (number of elements hint passed to the filter during construction was 0).");
            }
            if (isActivated) {
                throw new IOException("The bloomfilter already exists!");
            }
            HashTo128Bit.hashto128(by, SEED, hashes);
            for (int i = 0; i < numHashes; ++i) {
                long hash = Math.abs((hashes[0] + i * hashes[1]) % numBits);
                int pageNo = (int) hash / numBitsPerPage;
                int byteNo = (int) (hash % numBitsPerPage) >> 3;
                byte b = pages[pageNo].read(byteNo);
                int bitNo = (int) (hash % numBitsPerPage) & 0x07;
                b = (byte) (b | (1 << bitNo));
                pages[pageNo].put(byteNo, b);
            }
        }

        public void write() throws IOException {
            write(bloomFile);
        }

        public void write(File file) throws IOException {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
            out.write(intToBytes(numHashes));
            out.write(intToBytes(numElements));
            out.write(intToBytes(numPages));
            out.write(longToBytes(numBits));
            for (int i = 0; i < numPages; i++) {
                out.write(pages[i].getBuffer());
            }
            out.close();
        }

        private byte[] intToBytes(int v) {
            byte[] dest = new byte[4];
            dest[0] = (byte) (v & 0xFF);
            for (int k = 1; k < 4; k++) {
                dest[k] = (byte) ((v >> 8) & 0xFF);
                v = v >> 8;
            }
            return dest;
        }

        private byte[] longToBytes(long v) {
            byte[] dest = new byte[8];
            dest[0] = (byte) (v & 0xFF);
            for (int k = 1; k < 8; k++) {
                dest[k] = (byte) ((v >> 8) & 0xFF);
                v = v >> 8;
            }
            return dest;
        }
    }
}

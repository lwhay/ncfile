/**
 * 
 */
package utils.disk;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

/**
 * @author Michael
 *
 */
public class Reader implements Runnable {
    private static final String PREFIX = "tmp_sf";

    private final int id;
    private final int opst;
    private final int gran;
    private final long size;
    private final int rang;
    private long[][] offsets;
    private static boolean totalOrder = false;

    //private final int parallelism;
    private BitSet bs;
    private byte[] cache;

    private void seriesRead() throws IOException {
        System.out.println("type: " + opst + " gran: " + gran + " size: " + size + " rang: " + rang);
        switch (opst) {
            case 0: {
                // Initialize a file.
                long off = 0;
                cache = new byte[gran];
                FileOutputStream os = new FileOutputStream(new File(PREFIX + id + ".raw"));
                int count = 0;
                while (off < size) {
                    Arrays.fill(cache, (byte) (count % 0x7f));
                    os.write(cache);
                    off += cache.length;
                    count++;
                }
                System.out.println("Write count: " + count);
                os.close();
                break;
            }
            case 1: {
                // Sequential read;
                FileInputStream is = new FileInputStream(new File(PREFIX + id + ".raw"));
                long len = new File(PREFIX + id + ".raw").length();
                long off = 0;
                long round = 0;
                int read = 0;
                int count = 0;
                double skipping = .0;
                long oldp = 0;
                cache = new byte[gran];
                long begin = System.currentTimeMillis();
                while (off < size) {
                    if (round + gran > len) {
                        System.out.println("\t " + is.markSupported() + "\t" + cache[0] + " now at: " + off);
                        if (is.markSupported()) {
                            is.reset();
                        } else {
                            is.close();
                            is = new FileInputStream(new File(PREFIX + id + ".raw"));
                        }
                        round = 0;
                    }
                    //System.out.println(is.getChannel().position());
                    read = is.read(cache);
                    long newp = is.getChannel().position();
                    if (newp - oldp > gran) {
                        skipping += (newp - oldp - gran) / gran;
                    }
                    oldp = newp;
                    off += read;
                    round += read;
                    count++;
                }
                is.close();
                System.out.println("\n" + "Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                        + " skipping: " + skipping / count);
                break;
            }
            case 2: {
                // Random sequential read;
                RandomAccessFile raf = new RandomAccessFile(PREFIX + id + ".raw", "r");
                long len = new File(PREFIX + id + ".raw").length();
                long off = 0;
                long round = 0;
                int read = 0;
                int count = 0;
                double skipping = .0;
                long oldp = 0;
                cache = new byte[gran];
                long begin = System.currentTimeMillis();
                while (off < size) {
                    if (round + gran > len) {
                        System.out.println("\t " + raf.getFilePointer() + "\t" + cache[0] + " now at: " + off);
                        round = 0;
                    }
                    raf.seek(round);
                    //System.out.println(round);
                    read = raf.read(cache);
                    long newp = raf.getChannel().position();
                    if (newp - oldp > gran) {
                        skipping += (newp - oldp - gran) / gran;
                    }
                    oldp = newp;
                    off += read;
                    round += read;
                    count++;
                }
                raf.close();
                System.out.println("\n" + "Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                        + " skipping: " + skipping / count);
                break;
            }
            case 3: {
                // Sequential read;
                FileInputStream is = new FileInputStream(new File(PREFIX + id + ".raw"));
                long len = new File(PREFIX + id + ".raw").length();
                long off = 0;
                long round = 0;
                int read = 0;
                int count = 0;
                double skipping = .0;
                long oldp = 0;
                cache = new byte[gran];
                FileChannel chan = is.getChannel();
                ByteBuffer buf = ByteBuffer.wrap(cache, 0, cache.length);
                long begin = System.currentTimeMillis();
                while (off < size) {
                    buf.rewind();
                    if (round + gran > len) {
                        System.out.println("\t " + is.markSupported() + "\t" + cache[0] + " now at: " + off);
                        if (is.markSupported()) {
                            is.reset();
                        } else {
                            is.close();
                            is = new FileInputStream(new File(PREFIX + id + ".raw"));
                        }
                        round = 0;
                    }
                    //System.out.println(off);
                    read = chan.read(buf, off);
                    long newp = off;
                    if (newp - oldp > gran) {
                        skipping += (newp - oldp - gran) / gran;
                    }
                    oldp = newp;
                    off += read;
                    round += read;
                    count++;
                }
                chan.close();
                is.close();
                System.out.println("\n" + "Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                        + " skipping: " + skipping / count);
                break;
            }
            case 4: {
                // Sequentially fixed-size skipping read;
                // There exists bug to skip onto the non-touched blocks.
                FileInputStream is = new FileInputStream(new File(PREFIX + id + ".raw"));
                bs = new BitSet((int) (size / gran));
                long off = 0;
                long step = gran * rang;
                int count = 0;
                double skipping = .0;
                long oldp = 0;
                cache = new byte[gran];
                offsets = new long[(int) (rang)][];
                for (int i = 0; i < rang; i++) {
                    offsets[i] = new long[(int) (size / step)];
                    if (!totalOrder) {
                        for (int j = 0; j < size / step; j++) {
                            offsets[i][j] = (rang - 1 - i) * gran + j * step;
                        }
                    } else {
                        for (int j = 0; j < size / step; j++) {
                            offsets[i][j] = i * gran + j * step;
                        }
                    }
                }
                long begin = System.currentTimeMillis();
                for (int i = 0; i < rang; i++) {
                    is.close();
                    is = new FileInputStream(new File(PREFIX + id + ".raw"));
                    long oldpos = 0;
                    for (int j = 0; j < size / step; j++) {
                        off = offsets[i][j] - oldpos;
                        //if (off > 0) {
                        is.skip(off);
                        //}
                        //System.out.println(is.getChannel().position());
                        is.read(cache);
                        long newp = is.getChannel().position();
                        if (newp - oldp > gran) {
                            skipping += (newp - oldp - gran) / gran;
                        }
                        oldp = newp;
                        oldpos = offsets[i][j] + gran;
                        count++;
                    }
                    if (i % (rang / 10 + 1) == 0) {
                        System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                + " round: " + i);
                    }
                }
                is.close();
                System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                        + " round: " + rang + " skipping: " + skipping / count);
                break;
            }
            case 5: {
                // Randomly fixed-size skipping read;
                RandomAccessFile raf = new RandomAccessFile(PREFIX + id + ".raw", "r");
                bs = new BitSet((int) (size / gran));
                long step = gran * rang;
                int count = 0;
                double skipping = .0;
                long oldp = 0;
                cache = new byte[gran];
                offsets = new long[(int) (rang)][];
                for (int i = 0; i < rang; i++) {
                    offsets[i] = new long[(int) (size / step)];
                    if (!totalOrder) {
                        for (int j = 0; j < size / step; j++) {
                            offsets[i][j] = (rang - 1 - i) * gran + j * step;
                        }
                    } else {
                        for (int j = 0; j < size / step; j++) {
                            offsets[i][j] = i * gran + j * step;
                        }
                    }
                }
                long begin = System.currentTimeMillis();
                for (int i = 0; i < rang; i++) {
                    for (int j = 0; j < size / step; j++) {
                        raf.seek(offsets[i][j]);
                        //System.out.println(offsets[i][j]);
                        raf.read(cache);
                        long newp = raf.getChannel().position();
                        if (newp - oldp > gran) {
                            skipping += (newp - oldp - gran) / gran;
                        }
                        oldp = newp;
                        count++;
                    }
                    if (i % (rang / 10 + 1) == 0) {
                        System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                + " round: " + i);
                    }
                }
                raf.close();
                System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                        + " round: " + rang + " skipping: " + skipping / count);
                break;
            }
            case 6: {
                // Sequential read;
                FileInputStream is = new FileInputStream(new File(PREFIX + id + ".raw"));
                bs = new BitSet((int) (size / gran));
                long step = gran * rang;
                int count = 0;
                double skipping = .0;
                long oldp = 0;
                cache = new byte[gran];
                offsets = new long[(int) (rang)][];
                ByteBuffer buf = ByteBuffer.wrap(cache, 0, cache.length);
                for (int i = 0; i < rang; i++) {
                    offsets[i] = new long[(int) (size / step)];
                    if (!totalOrder) {
                        for (int j = 0; j < size / step; j++) {
                            offsets[i][j] = (rang - 1 - i) * gran + j * step;
                        }
                    } else {
                        for (int j = 0; j < size / step; j++) {
                            offsets[i][j] = i * gran + j * step;
                        }
                    }
                }
                FileChannel chan = is.getChannel();
                long begin = System.currentTimeMillis();
                for (int i = 0; i < rang; i++) {
                    for (int j = 0; j < size / step; j++) {
                        buf.rewind();
                        //System.out.println(offsets[i][j]);
                        chan.read(buf, offsets[i][j]);
                        long newp = offsets[i][j];
                        if (newp - oldp > gran) {
                            skipping += (newp - oldp - gran) / gran;
                        }
                        oldp = newp;
                        count++;
                    }
                    if (i % (rang / 10 + 1) == 0) {
                        System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                + " round: " + i);
                    }
                }
                chan.close();
                is.close();
                System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                        + " round: " + rang + " skipping: " + skipping / count);
                break;
            }
            case 7: {
                // Sequentially random skipping read;
                // There exists bug to skip onto the non-touched blocks.
                FileInputStream is = new FileInputStream(new File(PREFIX + id + ".raw"));
                Random rnd = new Random();
                long len = new File(PREFIX + id + ".raw").length();
                bs = new BitSet((int) (size / gran));
                long off = 0;
                long round = 0;
                long step = 0;
                int count = 0;
                double skipping = .0;
                long oldp = 0;
                cache = new byte[gran];
                offsets = new long[(int) (1)][];
                offsets[0] = new long[(int) (size / gran) + 1];
                int totalClearing = 0;
                while (off < size) {
                    step = Math.abs((rnd.nextInt() % (rang * 2)) * gran) + gran;
                    int desiredBlockId = (int) ((round + step) / gran);
                    round += step;
                    if (round + gran > size) {
                        round = 0;
                        desiredBlockId = 0;
                        totalClearing++;
                        //System.exit(-1);
                    }
                    // Look for an unused block;
                    while (bs.get(desiredBlockId)) {
                        if (round + gran > len) {
                            System.out.println("\t " + cache[0] + " now at: " + (round + step) + " for: " + off);
                            System.exit(-1);
                        }
                        round += gran;
                        desiredBlockId = (int) ((round) / gran);
                    }
                    offsets[0][count++] = round;
                    bs.set(desiredBlockId);
                    // Suppose we read here, and the round will be forwarded by 1 block.
                    // Enable this accumulation using a minimal step 1 in line 280, denoting at least we will skip one block.
                    //round += gran;
                    //System.out.println(round);
                    off += gran;
                }
                long begin = System.currentTimeMillis();
                long oldpos = 0;
                step = 0;
                int cleared = 0;
                count = 0;
                //is.skip(offsets[0][0]);
                for (int i = 0; i < offsets[0].length; i++) {
                    if (offsets[0][i] < oldpos) {
                        if (cleared % (totalClearing / 10 + 1) == 0) {
                            /*System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                    + " round: " + cleared);*/
                        }
                        is.close();
                        is = new FileInputStream(new File(PREFIX + id + ".raw"));
                        oldpos = 0;
                        cleared++;
                    }
                    step = offsets[0][i] - oldpos;
                    if (step > 0) {
                        is.skip(step);
                    }
                    //System.out.println(is.getChannel().position());
                    is.read(cache);
                    long newp = is.getChannel().position();
                    if (newp - oldp > gran) {
                        skipping += (newp - oldp - gran) / gran;
                    }
                    oldp = newp;
                    oldpos = offsets[0][i] + gran;
                    count++;
                }
                is.close();
                System.out.println("Read count: " + count + " real count: " + bs.cardinality() + " Time: "
                        + (System.currentTimeMillis() - begin) + " round: " + cleared + " skipping: "
                        + skipping / count);
                break;
            }
            case 8: {
                // Randomly random skipping read;
                RandomAccessFile raf = new RandomAccessFile(PREFIX + id + ".raw", "r");
                Random rnd = new Random();
                long len = new File(PREFIX + id + ".raw").length();
                bs = new BitSet((int) (size / gran));
                long off = 0;
                long round = 0;
                long step = 0;
                int count = 0;
                double skipping = .0;
                long oldp = 0;
                int totalClearing = 0;
                cache = new byte[gran];
                offsets = new long[(int) (1)][];
                offsets[0] = new long[(int) (size / gran + 1)];
                while (off < size) {
                    step = Math.abs((rnd.nextInt() % (rang * 2)) * gran) + gran;
                    int desiredBlockId = (int) ((round + step) / gran);
                    round += step;
                    if (round + gran > size) {
                        round = 0;
                        desiredBlockId = 0;
                        totalClearing++;
                    }
                    // Look for an unused block;
                    while (bs.get(desiredBlockId)) {
                        if (round + gran > len) {
                            System.out.println("\t " + raf.getFilePointer() + "\t" + cache[0] + " now at: "
                                    + (round + step) + " for: " + off);
                            //System.exit(-1);
                        }
                        round += gran;
                        desiredBlockId = (int) ((round) / gran);
                    }
                    offsets[0][count++] = round;
                    bs.set(desiredBlockId);
                    // Suppose we read here, and the round will be forwarded by 1 block.
                    // Enable this accumulation using a minimal step 1 in line 280, denoting at least we will skip one block.
                    round += gran;
                    off += gran;
                }
                long oldpos = 0;
                int cleared = 0;
                count = 0;
                long begin = System.currentTimeMillis();
                for (int i = 0; i < offsets[0].length; i++) {
                    if (offsets[0][i] < oldpos) {
                        if (cleared++ % (totalClearing / 10 + 1) == 0) {
                            /*System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                    + " round: " + cleared);*/
                        }
                    }
                    //System.out.println(offsets[0][i]);
                    raf.seek(offsets[0][i]);
                    raf.read(cache);
                    long newp = raf.getChannel().position();
                    if (newp - oldp > gran) {
                        skipping += (newp - oldp - gran) / gran;
                    }
                    oldp = newp;
                    oldpos = offsets[0][i] + gran;
                    count++;
                }
                raf.close();
                System.out.println("Read count: " + count + " real count: " + bs.cardinality() + " Time: "
                        + (System.currentTimeMillis() - begin) + " round: " + cleared + " skipping: "
                        + skipping / count);
                break;
            }
            case 9: {
                // Sequential read;
                FileInputStream is = new FileInputStream(new File(PREFIX + id + ".raw"));
                Random rnd = new Random();
                long len = new File(PREFIX + id + ".raw").length();
                bs = new BitSet((int) (size / gran));
                long off = 0;
                long round = 0;
                int count = 0;
                double skipping = .0f;
                long oldp = 0;
                long step = 0;
                cache = new byte[gran];
                int totalClearing = 0;
                FileChannel chan = is.getChannel();
                ByteBuffer buf = ByteBuffer.wrap(cache, 0, cache.length);
                offsets = new long[(int) (1)][];
                offsets[0] = new long[(int) (size / gran) + 1];
                while (off < size) {
                    step = Math.abs((rnd.nextInt() % (rang * 2)) * gran) + gran;
                    int desiredBlockId = (int) ((round + step) / gran);
                    round += step;
                    if (round + gran > size) {
                        round = 0;
                        desiredBlockId = 0;
                        totalClearing++;
                    }
                    // Look for an unused block;
                    while (bs.get(desiredBlockId)) {
                        if (round + gran > len) {
                            System.out.println("\t " + chan.position() + "\t" + cache[0] + " now at: " + (round + step)
                                    + " for: " + off);
                            //System.exit(-1);
                        }
                        round += gran;
                        desiredBlockId = (int) ((round) / gran);
                    }
                    offsets[0][count++] = round;
                    bs.set(desiredBlockId);
                    // Suppose we read here, and the round will be forwarded by 1 block.
                    // Enable this accumulation using a minimal step 1 in line 280, denoting at least we will skip one block.
                    round += gran;
                    off += gran;
                }
                int cleared = 0;
                long oldpos = 0;
                count = 0;
                //System.out.println("Begin read");
                long begin = System.currentTimeMillis();
                for (int i = 0; i < offsets[0].length; i++) {
                    if (offsets[0][i] < oldpos) {
                        if (cleared++ % (totalClearing / 10 + 1) == 0) {
                            /*System.out.println("Read count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                    + " round: " + cleared);*/
                        }
                    }
                    buf.rewind();
                    //System.out.println(offsets[0][i]);
                    chan.read(buf, offsets[0][i]);
                    long newp = offsets[0][i];
                    if (newp - oldp > gran) {
                        skipping += (newp - oldp - gran) / gran;
                    }
                    oldp = newp;
                    oldpos = offsets[0][i] + gran;
                    count++;
                }
                chan.close();
                is.close();
                System.out.println("Read count: " + count + " real count: " + bs.cardinality() + " Time: "
                        + (System.currentTimeMillis() - begin) + " round: " + cleared + " skipping: "
                        + skipping / count);
                break;
            }
            default:
                break;
        }
    }

    public Reader(int id, int opts, int gran, long size, int range) {
        this.id = id;
        this.opst = opts;
        this.gran = gran;
        this.size = size;
        this.rang = range;
    }

    @Override
    public void run() {
        try {
            seriesRead();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

/*
 * Copyright 2007-2015 by The Regents of the Wuhan University of China.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package utils.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

/**
 * @author Administrator
 *
 */
public class StrictWrite {
    private static int opst;
    private static int gran;
    private static long size;
    private static int rang;
    private static final String PATH = "tmp_sf.raw";
    private static BitSet bs;
    private static byte[] cache;
    private static long[][] offsets;
    private static boolean totalOrder;

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        opst = Integer.parseInt(args[0]);
        gran = Integer.parseInt(args[1]);
        size = Long.parseLong(args[2]);
        rang = Integer.parseInt(args[3]);
        totalOrder = Boolean.parseBoolean(args[4]);
        System.out.println(
                "type: " + opst + " gran: " + gran + " size: " + size + " rang: " + rang + " order: " + totalOrder);
        switch (opst) {
            case 0: {
                // Initialize a file.
                long off = 0;
                cache = new byte[gran];
                FileOutputStream os = new FileOutputStream(new File(PATH));
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
                RandomAccessFile raf = new RandomAccessFile(new File(PATH), "rw");
                long len = new File(PATH).length();
                long off = 0;
                long round = 0;
                int count = 0;
                cache = new byte[gran];
                long begin = System.currentTimeMillis();
                MappedByteBuffer mb;
                while (off < size) {
                    Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                    if (round + gran > len) {
                        System.out.println("\t " + "\t" + cache[0] + " now at: " + off);
                        raf.close();
                        raf = new RandomAccessFile(new File(PATH), "rw");
                        round = 0;
                    }
                    mb = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, round, gran);
                    mb.put(cache);
                    mb.clear();
                    off += gran;
                    round += gran;
                    count++;
                }
                raf.close();
                System.out.println("\n" + "Write count: " + count + " Time: " + (System.currentTimeMillis() - begin));
                break;
            }
            case 2: {
                // Random sequential read;
                RandomAccessFile raf = new RandomAccessFile(PATH, "rw");
                long len = new File(PATH).length();
                long off = 0;
                long round = 0;
                int count = 0;
                cache = new byte[gran];
                long begin = System.currentTimeMillis();
                while (off < size) {
                    Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                    if (round + gran > len) {
                        System.out.println("\t " + raf.getFilePointer() + "\t" + cache[0] + " now at: " + off);
                        round = 0;
                    }
                    raf.seek(round);
                    raf.write(cache);
                    off += gran;
                    round += gran;
                    count++;
                }
                raf.close();
                System.out.println("\n" + "Write count: " + count + " Time: " + (System.currentTimeMillis() - begin));
                break;
            }
            case 3: {
                // Sequential read;
                // Can we use stream instead of random file accessor?
                //FileOutputStream os = new FileOutputStream(new File(PATH));
                RandomAccessFile raf = new RandomAccessFile(PATH, "rw");
                long len = new File(PATH).length();
                long off = 0;
                long round = 0;
                int count = 0;
                cache = new byte[gran];
                FileChannel chan = raf.getChannel();
                ByteBuffer buf = ByteBuffer.wrap(cache, 0, cache.length);
                long begin = System.currentTimeMillis();
                while (off < size) {
                    Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                    buf = ByteBuffer.wrap(cache, 0, cache.length);
                    if (round + gran > len) {
                        System.out.println("\t " + "\t" + cache[0] + " now at: " + off);
                        chan.close();
                        raf.close();
                        raf = new RandomAccessFile(PATH, "rw");
                        chan = raf.getChannel();
                        round = 0;
                    }
                    chan.write(buf, off);
                    off += gran;
                    round += gran;
                    count++;
                }
                chan.close();
                raf.close();
                System.out.println("\n" + "Write count: " + count + " Time: " + (System.currentTimeMillis() - begin));
                break;
            }
            case 4: {
                // Sequentially fixed-size skipping read;
                // There exists bug to skip onto the non-touched blocks.
                RandomAccessFile raf = new RandomAccessFile(PATH, "rw");
                bs = new BitSet((int) (size / gran));
                long step = gran * rang;
                int count = 0;
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
                    raf.close();
                    raf = new RandomAccessFile(PATH, "rw");
                    for (int j = 0; j < size / step; j++) {
                        Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                        MappedByteBuffer mb = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, offsets[i][j], gran);
                        mb.put(cache);
                        mb.clear();
                        count++;
                    }
                    if (i % (rang / 10 + 1) == 0) {
                        System.out.println("Write count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                + " round: " + i);
                    }
                }
                raf.close();
                System.out.println(
                        "Read count: " + count + " Time: " + (System.currentTimeMillis() - begin) + " round: " + rang);
                break;
            }
            case 5: {
                RandomAccessFile raf = new RandomAccessFile(PATH, "rw");
                bs = new BitSet((int) (size / gran));
                long step = gran * rang;
                int count = 0;
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
                        Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                        raf.seek(offsets[i][j]);
                        raf.write(cache);
                        count++;
                    }
                    if (i % (rang / 10 + 1) == 0) {
                        System.out.println("Write count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                + " round: " + i);
                    }
                }
                raf.close();
                System.out.println(
                        "Write count: " + count + " Time: " + (System.currentTimeMillis() - begin) + " round: " + rang);
                break;
            }
            case 6: {
                RandomAccessFile raf = new RandomAccessFile(PATH, "rw");
                bs = new BitSet((int) (size / gran));
                long step = gran * rang;
                int count = 0;
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
                FileChannel chan = raf.getChannel();
                long begin = System.currentTimeMillis();
                for (int i = 0; i < rang; i++) {
                    for (int j = 0; j < size / step; j++) {
                        Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                        buf = ByteBuffer.wrap(cache, 0, cache.length);
                        chan.write(buf, offsets[i][j]);
                        count++;
                    }
                    if (i % (rang / 10 + 1) == 0) {
                        System.out.println("Write count: " + count + " Time: " + (System.currentTimeMillis() - begin)
                                + " round: " + i);
                    }
                }
                chan.close();
                raf.close();
                System.out.println(
                        "Write count: " + count + " Time: " + (System.currentTimeMillis() - begin) + " round: " + rang);
                break;
            }
            case 7: {
                // Sequentially random skipping read;
                // There exists bug to skip onto the non-touched blocks.
                RandomAccessFile raf = new RandomAccessFile(PATH, "rw");
                Random rnd = new Random();
                long len = new File(PATH).length();
                bs = new BitSet((int) (size / gran));
                long off = 0;
                long round = 0;
                long step = 0;
                int count = 0;
                cache = new byte[gran];
                offsets = new long[(int) (1)][];
                offsets[0] = new long[(int) (size / gran)];
                int totalClearing = 0;
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
                            System.out.println("\t " + "\t" + cache[0] + " now at: " + (round + step) + " for: " + off);
                            System.exit(-1);
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
                long begin = System.currentTimeMillis();
                long oldpos = 0;
                int cleared = 0;
                count = 0;
                for (int i = 0; i < offsets[0].length; i++) {
                    Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                    if (offsets[0][i] < oldpos) {
                        if (cleared % (totalClearing / 10 + 1) == 0) {
                            System.out.println("Write count: " + count + " Time: "
                                    + (System.currentTimeMillis() - begin) + " round: " + cleared);
                        }
                        oldpos = 0;
                        cleared++;
                    }
                    MappedByteBuffer mb = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, offsets[0][i], gran);
                    mb.put(cache);
                    mb.clear();
                    oldpos = offsets[0][i];
                    count++;
                }
                raf.close();
                System.out.println("Write count: " + count + " real count: " + bs.cardinality() + " Time: "
                        + (System.currentTimeMillis() - begin) + " round: " + cleared);
                break;
            }
            case 8: {
                // Randomly random skipping read;
                RandomAccessFile raf = new RandomAccessFile(PATH, "rw");
                Random rnd = new Random();
                long len = new File(PATH).length();
                bs = new BitSet((int) (size / gran));
                long off = 0;
                long round = 0;
                long step = 0;
                int count = 0;
                int totalClearing = 0;
                cache = new byte[gran];
                offsets = new long[(int) (1)][];
                offsets[0] = new long[(int) (size / gran)];
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
                            System.exit(-1);
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
                    Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                    if (offsets[0][i] < oldpos) {
                        if (cleared++ % (totalClearing / 10 + 1) == 0) {
                            System.out.println("Write count: " + count + " Time: "
                                    + (System.currentTimeMillis() - begin) + " round: " + cleared);
                        }
                    }
                    raf.seek(offsets[0][i]);
                    raf.write(cache);
                    oldpos = offsets[0][i];
                    count++;
                }
                raf.close();
                System.out.println("Write count: " + count + " real count: " + bs.cardinality() + " Time: "
                        + (System.currentTimeMillis() - begin) + " round: " + cleared);
                break;
            }
            case 9: {
                // Sequential read;
                RandomAccessFile raf = new RandomAccessFile(PATH, "rw");
                Random rnd = new Random();
                long len = new File(PATH).length();
                bs = new BitSet((int) (size / gran));
                long off = 0;
                long round = 0;
                int count = 0;
                long step = 0;
                cache = new byte[gran];
                int totalClearing = 0;
                FileChannel chan = raf.getChannel();
                ByteBuffer buf = ByteBuffer.wrap(cache, 0, cache.length);
                offsets = new long[(int) (1)][];
                offsets[0] = new long[(int) (size / gran)];
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
                                    + " for: " + off + " file len: " + len);
                            System.exit(-1);
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
                long begin = System.currentTimeMillis();
                for (int i = 0; i < offsets[0].length; i++) {
                    Arrays.fill(cache, (byte) ((count + 1) % 0x7f));
                    buf = ByteBuffer.wrap(cache, 0, cache.length);
                    if (offsets[0][i] < oldpos) {
                        if (cleared++ % (totalClearing / 10 + 1) == 0) {
                            System.out.println("Write count: " + count + " Time: "
                                    + (System.currentTimeMillis() - begin) + " round: " + cleared);
                        }
                    }
                    chan.write(buf, offsets[0][i]);
                    oldpos = offsets[0][i];
                    count++;
                }
                chan.close();
                raf.close();
                System.out.println("Write count: " + count + " real count: " + bs.cardinality() + " Time: "
                        + (System.currentTimeMillis() - begin) + " round: " + cleared);
                break;
            }
            default:
                break;
        }
    }
}

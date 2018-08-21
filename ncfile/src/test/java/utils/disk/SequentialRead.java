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
 * @author michael
 *
 */
public class SequentialRead {
    private static int opst;
    private static int gran;
    private static long size;
    private static int rang;
    private static final String PATH = "tmp_sf.raw";
    private static BitSet bs;
    private static byte[] cache;

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        opst = Integer.parseInt(args[0]);
        gran = Integer.parseInt(args[1]);
        size = Long.parseLong(args[2]);
        rang = Integer.parseInt(args[3]);
        System.out.println("type: " + opst + " gran: " + gran + " size: " + size + " rang: " + rang);
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
                FileInputStream is = new FileInputStream(new File(PATH));
                long len = new File(PATH).length();
                long off = 0;
                long round = 0;
                int read = 0;
                int count = 0;
                cache = new byte[gran];
                while (off < size) {
                    if (round + gran > len) {
                        System.out.println("\t " + is.markSupported() + "\t" + cache[0] + " now at: " + off);
                        if (is.markSupported()) {
                            is.reset();
                        } else {
                            is.close();
                            is = new FileInputStream(new File(PATH));
                        }
                        round = 0;
                    }
                    read = is.read(cache);
                    off += read;
                    round += read;
                    count++;
                }
                is.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            case 2: {
                // Random sequential read;
                RandomAccessFile raf = new RandomAccessFile(PATH, "r");
                long len = new File(PATH).length();
                long off = 0;
                long round = 0;
                int read = 0;
                int count = 0;
                cache = new byte[gran];
                while (off < size) {
                    if (round + gran > len) {
                        System.out.println("\t " + raf.getFilePointer() + "\t" + cache[0] + " now at: " + off);
                        round = 0;
                    }
                    raf.seek(round);
                    read = raf.read(cache);
                    off += read;
                    round += read;
                    count++;
                }
                raf.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            case 3: {
                // Sequential read;
                FileInputStream is = new FileInputStream(new File(PATH));
                long len = new File(PATH).length();
                long off = 0;
                long round = 0;
                int read = 0;
                int count = 0;
                cache = new byte[gran];
                FileChannel chan = is.getChannel();
                ByteBuffer buf = ByteBuffer.wrap(cache, 0, cache.length);
                while (off < size) {
                    buf.rewind();
                    if (round + gran > len) {
                        System.out.println("\t " + is.markSupported() + "\t" + cache[0] + " now at: " + off);
                        if (is.markSupported()) {
                            is.reset();
                        } else {
                            is.close();
                            is = new FileInputStream(new File(PATH));
                        }
                        round = 0;
                    }
                    read = chan.read(buf, off);
                    off += read;
                    round += read;
                    count++;
                }
                is.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            case 4: {
                // Sequentially fixed-size skipping read;
                // There exists bug to skip onto the non-touched blocks.
                FileInputStream is = new FileInputStream(new File(PATH));
                long len = new File(PATH).length();
                bs = new BitSet((int) (len / gran));
                long off = 0;
                long round = 0;
                long step = gran * rang;
                int read = 0;
                int count = 0;
                cache = new byte[gran];
                while (off < size) {
                    if (round + step + gran > len) {
                        System.out.println("\t " + is.markSupported() + "\t" + cache[0] + " now at: " + (round + step)
                                + " for: " + off);
                        if (is.markSupported()) {
                            is.reset();
                        } else {
                            is.close();
                            is = new FileInputStream(new File(PATH));
                        }
                        round = 0;
                    }
                    int desiredBlockId = (int) ((round + step) / gran);
                    while (bs.get(desiredBlockId)) {
                        if (round + step + gran > len) {
                            System.out.println("\t " + is.markSupported() + "\t" + cache[0] + " now at: "
                                    + (round + step) + " for: " + off);
                            if (is.markSupported()) {
                                is.reset();
                            } else {
                                is.close();
                                is = new FileInputStream(new File(PATH));
                            }
                            round = 0;
                        }
                        round += gran;
                        desiredBlockId = (int) ((round + step) / gran);
                    }
                    is.skip(step);
                    read = is.read(cache);
                    bs.set(desiredBlockId);
                    off += read;
                    round += step;
                    round += read;
                    count++;
                }
                is.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            case 5: {
                // Randomly fixed-size skipping read;
                RandomAccessFile raf = new RandomAccessFile(PATH, "r");
                long len = new File(PATH).length();
                bs = new BitSet((int) (len / gran));
                long off = 0;
                long round = 0;
                long step = gran * rang;
                int read = 0;
                int count = 0;
                cache = new byte[gran];
                while (off < size) {
                    if (round + step + gran > len) {
                        System.out.println("\t " + raf.getFilePointer() + "\t" + cache[0] + " now at: " + (round + step)
                                + " for: " + off);
                        round = 0;
                    }
                    int desiredBlockId = (int) ((round + step) / gran);
                    while (bs.get(desiredBlockId)) {
                        if (round + step + gran > len) {
                            System.out.println("\t " + raf.getFilePointer() + "\t" + cache[0] + " now at: "
                                    + (round + step) + " for: " + off);
                            round = 0;
                        }
                        round += gran;
                        desiredBlockId = (int) ((round + step) / gran);
                    }
                    round += step;
                    raf.seek(round);
                    read = raf.read(cache);
                    bs.set(desiredBlockId);
                    off += read;
                    round += read;
                    count++;
                }
                raf.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            case 6: {
                // Sequential read;
                FileInputStream is = new FileInputStream(new File(PATH));
                long len = new File(PATH).length();
                bs = new BitSet((int) (len / gran));
                long off = 0;
                long round = 0;
                int read = 0;
                int count = 0;
                long step = gran * rang;
                cache = new byte[gran];
                FileChannel chan = is.getChannel();
                ByteBuffer buf = ByteBuffer.wrap(cache, 0, cache.length);
                while (off < size) {
                    if (round + step + gran > len) {
                        System.out.println("\t " + chan.position() + "\t" + cache[0] + " now at: " + (round + step)
                                + " for: " + off);
                        round = 0;
                    }
                    int desiredBlockId = (int) ((round + step) / gran);
                    while (bs.get(desiredBlockId)) {
                        if (round + step + gran > len) {
                            System.out.println("\t " + chan.position() + "\t" + cache[0] + " now at: " + (round + step)
                                    + " for: " + off);
                            round = 0;
                        }
                        round += gran;
                        desiredBlockId = (int) ((round + step) / gran);
                    }
                    buf.rewind();
                    round += step;
                    read = chan.read(buf, round);
                    bs.set(desiredBlockId);
                    off += read;
                    round += read;
                    count++;
                }
                is.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            case 7: {
                // Sequentially random skipping read;
                // There exists bug to skip onto the non-touched blocks.
                FileInputStream is = new FileInputStream(new File(PATH));
                Random rnd = new Random();
                long len = new File(PATH).length();
                bs = new BitSet((int) (len / gran));
                long off = 0;
                long round = 0;
                long step = 0;
                int read = 0;
                int count = 0;
                cache = new byte[gran];
                while (off < size) {
                    step = Math.abs((rnd.nextInt() % (rang * 2)) * gran) + 1;
                    if (round + step + gran > len) {
                        System.out.println("\t " + is.markSupported() + "\t" + cache[0] + " now at: " + (round + step)
                                + " for: " + off);
                        if (is.markSupported()) {
                            is.reset();
                        } else {
                            is.close();
                            is = new FileInputStream(new File(PATH));
                        }
                        round = 0;
                    }
                    int desiredBlockId = (int) ((round + step) / gran);
                    while (bs.get(desiredBlockId)) {
                        if (round + step + gran > len) {
                            System.out.println("\t " + is.markSupported() + "\t" + cache[0] + " now at: "
                                    + (round + step) + " for: " + off);
                            if (is.markSupported()) {
                                is.reset();
                            } else {
                                is.close();
                                is = new FileInputStream(new File(PATH));
                            }
                            round = 0;
                        }
                        round += gran;
                        desiredBlockId = (int) ((round + step) / gran);
                    }
                    is.skip(step);
                    read = is.read(cache);
                    bs.set(desiredBlockId);
                    off += read;
                    round += step;
                    round += read;
                    count++;
                }
                is.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            case 8: {
                // Randomly random skipping read;
                RandomAccessFile raf = new RandomAccessFile(PATH, "r");
                Random rnd = new Random();
                long len = new File(PATH).length();
                bs = new BitSet((int) (len / gran));
                long off = 0;
                long round = 0;
                long step = 0;
                int read = 0;
                int count = 0;
                cache = new byte[gran];
                while (off < size) {
                    step = Math.abs((rnd.nextInt() % (rang * 2)) * gran) + 1;
                    if (round + step + gran > len) {
                        System.out.println("\t " + raf.getFilePointer() + "\t" + cache[0] + " now at: " + (round + step)
                                + " for: " + off);
                        round = 0;
                    }
                    int desiredBlockId = (int) ((round + step) / gran);
                    while (bs.get(desiredBlockId)) {
                        if (round + step + gran > len) {
                            System.out.println("\t " + raf.getFilePointer() + "\t" + cache[0] + " now at: "
                                    + (round + step) + " for: " + off);
                            round = 0;
                        }
                        round += gran;
                        desiredBlockId = (int) ((round + step) / gran);
                    }
                    round += step;
                    raf.seek(round);
                    read = raf.read(cache);
                    bs.set(desiredBlockId);
                    off += read;
                    round += read;
                    count++;
                }
                raf.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            case 9: {
                // Sequential read;
                FileInputStream is = new FileInputStream(new File(PATH));
                Random rnd = new Random();
                long len = new File(PATH).length();
                bs = new BitSet((int) (len / gran));
                long off = 0;
                long round = 0;
                int read = 0;
                int count = 0;
                long step = 0;
                cache = new byte[gran];
                FileChannel chan = is.getChannel();
                ByteBuffer buf = ByteBuffer.wrap(cache, 0, cache.length);
                while (off < size) {
                    buf.rewind();
                    step = Math.abs((rnd.nextInt() % (rang * 2)) * gran) + 1;
                    if (round + step + gran > len) {
                        System.out.println("\t " + chan.position() + "\t" + cache[0] + " now at: " + (round + step)
                                + " for: " + off);
                        round = 0;
                    }
                    int desiredBlockId = (int) ((round + step) / gran);
                    while (bs.get(desiredBlockId)) {
                        if (round + step + gran > len) {
                            System.out.println("\t " + chan.position() + "\t" + cache[0] + " now at: " + (round + step)
                                    + " for: " + off);
                            round = 0;
                        }
                        round += gran;
                        desiredBlockId = (int) ((round + step) / gran);
                    }
                    round += step;
                    read = chan.read(buf, round);
                    bs.set(desiredBlockId);
                    off += read;
                    round += read;
                    count++;
                }
                is.close();
                System.out.println("\n" + "Read count: " + count);
                break;
            }
            default:
                break;
        }
    }

}

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

import java.io.IOException;

/**
 * @author Administrator
 *
 */
public class ParallelRead {

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        int opst = Integer.parseInt(args[0]);
        int gran = Integer.parseInt(args[1]);
        long size = Long.parseLong(args[2]);
        int range = Integer.parseInt(args[3]);
        int parallelism = Integer.parseInt(args[4]);
        Reader[] workers = new Reader[parallelism];
        Thread[] threads = new Thread[parallelism];
        long begin = System.currentTimeMillis();
        for (int i = 0; i < parallelism; i++) {
            workers[i] = new Reader(i, opst, gran, size, range);
            threads[i] = new Thread(workers[i]);
            threads[i].start();
        }

        for (int i = 0; i < parallelism; i++) {
            threads[i].join();
        }
        System.out.println("Total runtime: " + (System.currentTimeMillis() - begin));
    }
}

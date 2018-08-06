/**
 * 
 */
package io;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Michael
 *
 */
public class BlockInputBufferQueue extends ArrayBlockingQueue<PositionalBlock<Long, BlockInputBuffer>> {

    public BlockInputBufferQueue(int size) {
        super(size);
    }

    private static final long serialVersionUID = 1L;

}
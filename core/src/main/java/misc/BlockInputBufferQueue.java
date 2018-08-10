/**
 * 
 */
package misc;

import java.util.concurrent.ArrayBlockingQueue;

import io.BlockInputBuffer;

/**
 * @author Michael
 *
 */
public class BlockInputBufferQueue extends ArrayBlockingQueue<PositionalBlock<Integer, BlockInputBuffer>[]> {

    public BlockInputBufferQueue(int size) {
        super(size);
    }

    private static final long serialVersionUID = 1L;

}
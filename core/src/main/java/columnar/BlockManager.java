/**
 * 
 */
package columnar;

/**
 * @author Michael
 *
 */
public class BlockManager {
    public static final int DEFAULT_SCALE = 16;
    private final int blockSize;
    private final int cacheScale;

    public BlockManager(int bs, int cs) {
        this.blockSize = bs * 1024;
        this.cacheScale = cs;
    }

    public BlockManager(int bs) {
        this(bs, DEFAULT_SCALE);
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getCacheSize() {
        return cacheScale * blockSize;
    }

    public int getCacheScale() {
        return cacheScale;
    }
}

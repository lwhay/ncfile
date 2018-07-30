/**
 * 
 */
package misc;

import java.util.ArrayList;
import java.util.List;

import columnar.BlockDescriptor;

/**
 * @author Michael
 *
 */
public class Blocks {
    private List<BlockDescriptor> blocks;

    public Blocks() {
        blocks = new ArrayList<BlockDescriptor>();
    }

    public void add(BlockDescriptor b) {
        blocks.add(b);
    }

    public void clear() {
        blocks.clear();
    }

    public int size() {
        return blocks.size();
    }

    public BlockDescriptor get(int i) {
        return blocks.get(i);
    }
}

/**
 * 
 */
package misc;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Michael
 *
 */
public class ListArr {

    private List<Object> x;

    public ListArr() {
        this.x = new ArrayList<Object>();
    }

    public void add(Object o) {
        this.x.add(o);
    }

    public void clear() {
        x.clear();
    }

    public Object get(int i) {
        return x.get(i);
    }

    public Object[] toArray() {
        return x.toArray();
    }

    public int size() {
        return x.size();
    }
}

/**
 * 
 */
package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class PsizeEqualFilter implements FilterOperator<Integer> {
    private final int size;

    public PsizeEqualFilter(Integer size) {
        this.size = size;
    }

    @Override
    public String getName() {
        return "p_size";
    }

    @Override
    public boolean isMatch(Integer t) {
        return size == t;
    }

}

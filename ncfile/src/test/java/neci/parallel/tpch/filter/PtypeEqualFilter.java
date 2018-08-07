/**
 * 
 */
package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class PtypeEqualFilter implements FilterOperator<String> {
    private final String type;

    public PtypeEqualFilter(String type) {
        this.type = type;
    }

    @Override
    public String getName() {
        return "p_type";
    }

    @Override
    public boolean isMatch(String t) {
        return t.compareTo(type) == 0;
    }

}

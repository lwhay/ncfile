/**
 * 
 */
package neci.parallel.tpch.filter;

import java.util.Set;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class LsuppkeyContainedbyFilter implements FilterOperator<Long> {
    private final Set<Long> suppkeys;

    public LsuppkeyContainedbyFilter(Set<Long> suppkeys) {
        this.suppkeys = suppkeys;
    }

    @Override
    public String getName() {
        return "l_suppkey";
    }

    @Override
    public boolean isMatch(Long t) {
        return suppkeys.contains(t);
    }
}

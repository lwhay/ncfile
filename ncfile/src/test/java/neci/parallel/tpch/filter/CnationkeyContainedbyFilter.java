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
public class CnationkeyContainedbyFilter implements FilterOperator<Integer> {
    private final Set<Integer> suppkeys;

    public CnationkeyContainedbyFilter(Set<Integer> suppkeys) {
        this.suppkeys = suppkeys;
    }

    @Override
    public String getName() {
        return "c_nationkey";
    }

    @Override
    public boolean isMatch(Integer t) {
        return suppkeys.contains(t);
    }
}

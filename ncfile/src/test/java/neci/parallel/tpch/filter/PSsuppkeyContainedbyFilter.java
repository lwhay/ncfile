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
public class PSsuppkeyContainedbyFilter implements FilterOperator<Long> {
    private final Set<Long> suppkeys;

    public PSsuppkeyContainedbyFilter(Set<Long> suppkeys) {
        this.suppkeys = suppkeys;
    }

    @Override
    public String getName() {
        return "ps_suppkey";
    }

    @Override
    public boolean isMatch(Long t) {
        return suppkeys.contains(t);
    }
}

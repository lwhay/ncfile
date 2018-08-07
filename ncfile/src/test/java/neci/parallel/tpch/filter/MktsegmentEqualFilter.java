/**
 * 
 */
package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class MktsegmentEqualFilter implements FilterOperator<String> {
    private final String mkt;

    public MktsegmentEqualFilter(String mkt) {
        this.mkt = mkt;
    }

    @Override
    public String getName() {
        return "c_mktsegment";
    }

    @Override
    public boolean isMatch(String t) {
        return mkt.compareTo(t) == 0;
    }

}

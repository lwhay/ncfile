/**
 * 
 */
package tpch.single;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class Q03_MktsegmentFilter implements FilterOperator<String> {
    private final String mkt;

    public Q03_MktsegmentFilter(String mkt) {
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

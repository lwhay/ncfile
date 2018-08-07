/**
 * 
 */
package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class Q10_OrderdateFilter implements FilterOperator<String> {
    String t1, t2;

    public Q10_OrderdateFilter(String t1, String t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    @Override
    public String getName() {
        return "o_orderdate";
    }

    @Override
    public boolean isMatch(String s) {
        if (s.compareTo(t1) >= 0 && s.compareTo(t2) < 0)
            return true;
        else
            return false;
    }

}

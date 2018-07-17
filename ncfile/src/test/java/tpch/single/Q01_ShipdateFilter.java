/**
 * 
 */
package tpch.single;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class Q01_ShipdateFilter implements FilterOperator<String> {
    String ts;

    public Q01_ShipdateFilter(String ts) {
        this.ts = ts;
    }

    @Override
    public String getName() {
        return "l_shipdate";
    }

    @Override
    public boolean isMatch(String t) {
        return t.compareTo(ts) <= 0;
    }

}

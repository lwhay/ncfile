/**
 * 
 */
package tpch.single;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class Q03_ShipdateFilter implements FilterOperator<String> {
    private final String date;

    public Q03_ShipdateFilter(String date) {
        this.date = date;
    }

    @Override
    public String getName() {
        return "l_shipdate";
    }

    @Override
    public boolean isMatch(String t) {
        return t.compareTo(date) > 0;
    }

}

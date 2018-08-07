/**
 * 
 */
package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class ShipdateLargerFilter implements FilterOperator<String> {
    private final String date;

    public ShipdateLargerFilter(String date) {
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

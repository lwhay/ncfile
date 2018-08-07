/**
 * 
 */
package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class OrderdateSmallerFilter implements FilterOperator<String> {
    private final String date;

    public OrderdateSmallerFilter(String date) {
        this.date = date;
    }

    @Override
    public String getName() {
        return "o_orderdate";
    }

    @Override
    public boolean isMatch(String t) {
        return t.compareTo(date) < 0;
    }

}

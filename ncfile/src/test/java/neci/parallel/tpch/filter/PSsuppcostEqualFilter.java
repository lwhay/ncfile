/**
 * 
 */
package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class PSsuppcostEqualFilter implements FilterOperator<Float> {
    private final Float minSuppcost;

    public PSsuppcostEqualFilter(float minSuppcost) {
        this.minSuppcost = minSuppcost;
    }

    @Override
    public String getName() {
        return "ps_supplycost";
    }

    @Override
    public boolean isMatch(Float t) {
        return this.minSuppcost.equals(t);
    }
}

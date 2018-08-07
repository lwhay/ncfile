/**
 * 
 */
package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class QuantitySmallerFilter implements FilterOperator<Float> {
    private final float quantity;

    public QuantitySmallerFilter(float quantity) {
        this.quantity = quantity;
    }

    @Override
    public String getName() {
        return "l_quantity";
    }

    @Override
    public boolean isMatch(Float t) {
        return t < quantity;
    }
}

package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

public class DiscountBetweenFilter implements FilterOperator<Float> {
    private final float left;
    private final float right;

    public DiscountBetweenFilter(float left, float right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public String getName() {
        return "l_discount";
    }

    @Override
    public boolean isMatch(Float t) {
        return t >= left && t <= right;
    }
}

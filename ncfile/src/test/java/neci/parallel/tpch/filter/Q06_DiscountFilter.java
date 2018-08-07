package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

public class Q06_DiscountFilter implements FilterOperator<Float> {
    float f1, f2;

    public Q06_DiscountFilter(float t1, float t2) {
        f1 = t1;
        f2 = t2;
    }

    public String getName() {
        return "l_discount";
    }

    public boolean isMatch(Float s) {
        if (s >= f1 && s <= f2)
            return true;
        else
            return false;
    }
}

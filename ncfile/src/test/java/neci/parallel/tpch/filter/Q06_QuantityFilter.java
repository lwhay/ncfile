package neci.parallel.tpch.filter;

import neci.ncfile.FilterOperator;

public class Q06_QuantityFilter implements FilterOperator<Float> {
    float f;

    public Q06_QuantityFilter(float f) {
        this.f = f;
    }

    public String getName() {
        return "l_quantity";
    }

    public boolean isMatch(Float s) {
        if (s < f)
            return true;
        else
            return false;
    }
}

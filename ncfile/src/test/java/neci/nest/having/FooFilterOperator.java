/**
 * 
 */
package neci.nest.having;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class FooFilterOperator implements FilterOperator<Integer> {
    private static final int key = 300;

    @Override
    public String getName() {
        return "a1";
    }

    @Override
    public boolean isMatch(Integer t) {
        return t < 2 * key;
    }

}

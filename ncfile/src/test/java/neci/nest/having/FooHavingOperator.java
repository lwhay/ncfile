/**
 * 
 */
package neci.nest.having;

import java.util.List;

import neci.ncfile.HavingOperator;

/**
 * @author Michael
 *
 */
public class FooHavingOperator implements HavingOperator<Integer> {

    @Override
    public String getName() {
        return "a1";
    }

    @Override
    public boolean isMatch(Integer t) {
        return true;
    }

    @Override
    public String getHavingName() {
        return "b1";
    }

    @Override
    public boolean isMath(List<Integer> group) {
        boolean includes = false;
        for (Integer element : group) {
            if (element == 3) {
                includes = true;
            }
        }
        return includes;
    }
}

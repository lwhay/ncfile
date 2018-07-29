/**
 * 
 */
package tpch.single;

import java.nio.ByteBuffer;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class Q10_ReturnflagFilter implements FilterOperator<ByteBuffer> {
    private final ByteBuffer f;

    public Q10_ReturnflagFilter(ByteBuffer f) {
        this.f = f;
    }

    @Override
    public String getName() {
        return "l_returnflag";
    }

    @Override
    public boolean isMatch(ByteBuffer t) {
        return f.compareTo(t) == 0;
    }

}

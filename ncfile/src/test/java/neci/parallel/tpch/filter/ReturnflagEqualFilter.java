/**
 * 
 */
package neci.parallel.tpch.filter;

import java.nio.ByteBuffer;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class ReturnflagEqualFilter implements FilterOperator<ByteBuffer> {
    private final ByteBuffer f;

    public ReturnflagEqualFilter(ByteBuffer f) {
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

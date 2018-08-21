/**
 * 
 */
package neci.parallel.tpch.filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import neci.ncfile.FilterOperator;

/**
 * @author Michael
 *
 */
public class PtypeLikeFilter implements FilterOperator<String> {
    private final Pattern pattern;

    public PtypeLikeFilter(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }

    @Override
    public String getName() {
        return "p_type";
    }

    @Override
    public boolean isMatch(String t) {
        Matcher m = pattern.matcher(t);
        if (m.find()) {
            return true;
        }
        return false;
    }

}

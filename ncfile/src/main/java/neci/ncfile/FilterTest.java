package neci.ncfile;

public class FilterTest implements FilterOperator<String> {
    public FilterTest() {

    }

    @Override
    public String getName() {
        return "l_shipDate";
    }

    @Override
    public boolean isMatch(String m) {
        return m.compareTo("1993-10-29") < 0;
    }
}

package neci.ncfile;

import java.util.List;

public interface HavingOperator<T> extends FilterOperator<T> {
    public String getHavingName();

    public boolean isMath(List<T> group);
}

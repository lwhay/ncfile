package neci.ncfile;

public interface FilterOperator<T> {
    public String getName();

    public boolean isMatch(T t);
}

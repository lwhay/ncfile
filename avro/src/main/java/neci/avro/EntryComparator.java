package neci.avro;

import java.util.Comparator;
import java.util.Map.Entry;

public class EntryComparator<K, V> implements Comparator<Entry<K, V>> {
    @Override
    public int compare(Entry<K, V> o1, Entry<K, V> o2) {
        Comparable<? super K> k1 = (Comparable<? super K>) o1.getKey();
        return k1.compareTo(o2.getKey());
    }
}

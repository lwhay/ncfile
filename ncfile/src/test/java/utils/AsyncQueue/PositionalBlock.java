/**
 * 
 */
package utils.AsyncQueue;

import java.util.Map.Entry;

/**
 * @author Michael
 *
 */
public class PositionalBlock<K, V> implements Entry<K, V> {
    K key;
    V value;

    public PositionalBlock(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V arg0) {
        return this.value = arg0;
    }
}

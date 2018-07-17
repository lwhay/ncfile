/**
 * 
 */
package tpch.single;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

/**
 * @author Michael
 *
 */
public class Q05 {

    public static List<Integer> extractNation(String nationPath, Schema nationSchema, String regionPath,
            Schema regionSchema, String regionName) {
        List<Integer> nationkeys = new ArrayList<>();
        return nationkeys;
    }

    public static List<int[]> extractSupplier(String suppPath, Schema suppSchema, List<Integer> nationkeys) {
        List<int[]> suppnationkeys = new ArrayList<>();
        return suppnationkeys;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}

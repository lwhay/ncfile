/**
 * 
 */
package utils.tpch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Michael
 *
 */
public class SupplierHelper {
    private final String regionName;

    private final BitSet indicators;

    private float minSuppCost = .0f;

    private static final String lineitemPath = "./src/resources/tpch/lineitem.tbl";

    private static final String regionPath = "./src/resources/tpch/region.tbl";

    private static final String nationPath = "./src/resources/tpch/nation.tbl";

    private static final String supplierPath = "./src/resources/tpch/supplier.tbl";

    private Map<Long, String[]> validSuppCosts = new HashMap<>();

    private Map<Integer, String> nationNames = new HashMap<>();

    public SupplierHelper(String region, BitSet indicators) {
        this.regionName = region;
        this.indicators = indicators;
    }

    public void init() throws IOException {
        long begin = System.currentTimeMillis();
        BufferedReader brg = new BufferedReader(new FileReader(regionPath));
        String line;
        Set<Integer> regionKeys = new HashSet<>();
        while ((line = brg.readLine()) != null) {
            String[] flds = line.split("\\|");
            if (flds[1].equals(regionName)) {
                regionKeys.add(Integer.parseInt(flds[0]));
            }
        }
        brg.close();
        BufferedReader brn = new BufferedReader(new FileReader(nationPath));
        Set<Integer> nationKeys = new HashSet<>();
        while ((line = brn.readLine()) != null) {
            String[] flds = line.split("\\|");
            if (regionKeys.contains(Integer.parseInt(flds[2]))) {
                nationKeys.add(Integer.parseInt(flds[0]));
                nationNames.put(Integer.parseInt(flds[0]), flds[1]);
            }
        }
        brn.close();
        BufferedReader brs = new BufferedReader(new FileReader(supplierPath));
        while ((line = brs.readLine()) != null) {
            String[] flds = line.split("\\|");
            if (nationKeys.contains(Integer.parseInt(flds[3]))) {
                String[] loads = new String[indicators.cardinality()];
                int idx = 0;
                for (int i = 0; i < flds.length; i++) {
                    if (indicators.get(i)) {
                        loads[idx++] = flds[i];
                    }
                }
                validSuppCosts.put(Long.parseLong(flds[0]), loads);
            }
        }
        brs.close();
        System.out.println("SuppCost initialization: " + (System.currentTimeMillis() - begin));
    }

    public boolean exists(long ps_suppkey) {
        if (validSuppCosts.size() == 0) {
            try {
                init();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (validSuppCosts.containsKey(ps_suppkey)) {
            return true;
        }
        return false;
    }

    public void add(long ps_suppkey, float ps_suppcost) {
        if (exists(ps_suppkey) && ps_suppcost < minSuppCost) {
            minSuppCost = ps_suppcost;
        }
    }

    public boolean isMinCost(float ps_suppcost) {
        if (ps_suppcost == minSuppCost) {
            return true;
        }
        return false;
    }

    public Map<Long, String[]> getValidSuppCosts() {
        return validSuppCosts;
    }

    public Map<Integer, String> getNationNames() {
        return nationNames;
    }

    @SuppressWarnings("unused")
    private static void lineitemVerifiedByQ01() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(lineitemPath));
        String line = "";
        int count = 0;
        while ((line = br.readLine()) != null) {
            String[] flds = line.split("\\|");
            if (flds[10].compareTo("1998-09-01") <= 0) {
                count++;
            }
        }
        br.close();
        System.out.println("Count: " + count);
    }

    private static void patternVerification() {
        String source = "abcBRASS";
        //Pattern pattern = Pattern.compile("^a[a-z]*BRASS$");
        //Pattern pattern = Pattern.compile("^a\\w+BRASS$");
        Pattern pattern = Pattern.compile("^a.*BRASS$");
        Matcher matcher = pattern.matcher(source);
        System.out.println(matcher.find());
    }

    public static void main(String[] args) throws IOException {
        patternVerification();
    }
}

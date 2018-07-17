package misc;
import java.io.IOException;

public class TmpTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        String s = "mmm123mm3m";
        StringBuilder ins = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) >= '0' && s.charAt(i) <= '9') {
                ins.append(s.charAt(i++));
                while (i < s.length() && s.charAt(i) >= '0' && s.charAt(i) <= '9') {
                    ins.append(s.charAt(i++));
                }
                break;
            }
        }
        int in = Integer.parseInt(ins.toString());
        System.out.println(in);
    }
}

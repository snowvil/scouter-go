import scouter.util.DateTimeHelper;
import java.util.TimeZone;

// Runs the original Java calendar; no date algorithm is reimplemented here.
public class DateOracle {
    public static void main(String[] args) {
        DateTimeHelper h = DateTimeHelper.getDateTimeHelper(TimeZone.getTimeZone(args[0]));
        long ms = Long.parseLong(args[1]);
        System.out.println(h.yyyymmdd(ms));
        System.out.println(h.getDateMillis(ms));
        System.out.println(h.yyyymmdd(args[2]));
        System.out.println(h.yyyymmdd(0));
        System.out.println(h.getDateMillis(0));
    }
}

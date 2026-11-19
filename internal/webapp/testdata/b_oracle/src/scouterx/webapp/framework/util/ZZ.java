package scouterx.webapp.framework.util;

// Oracle stub: only ipByteToString, copied verbatim from the pinned ZZ.java.
public class ZZ {
    public static String ipByteToString(byte[] ip) {
        if (ip == null)
            return "0.0.0.0";
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(ip[0] & 0xff);
            sb.append(".");
            sb.append(ip[1] & 0xff);
            sb.append(".");
            sb.append(ip[2] & 0xff);
            sb.append(".");
            sb.append(ip[3] & 0xff);
            return sb.toString();
        } catch (Throwable e) {
            return "0.0.0.0";
        }
    }
}

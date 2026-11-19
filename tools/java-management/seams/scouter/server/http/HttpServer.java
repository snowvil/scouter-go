// Seam: original HttpServer embeds Jetty; only the static remote-address helper is reproduced line for line.
package scouter.server.http;
import javax.servlet.http.HttpServletRequest;
import scouter.util.StringUtil;
public final class HttpServer {
    public static String getRemoteAddr(HttpServletRequest request) {
        String ip = request.getHeader("X-FORWARDED-FOR");
        if (ip == null) { return request.getRemoteAddr(); }
        String[] ips = StringUtil.split(ip, ',');
        if (ips.length > 0) { return ips[0]; }
        return request.getRemoteAddr();
    }
}

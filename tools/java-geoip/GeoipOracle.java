import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * geoip-api 1.3.1 LookupService(file, GEOIP_MEMORY_CACHE)로 합성 legacy GeoIP .dat를 열고
 * getLocation(InetAddress)를 호출한 결과를 JSON으로 기록한다. scouter.server GISDataUtil과 같은 생성자/옵션/호출이다.
 *
 * 입력 spec(UTF-8 text): "db &lt;file&gt;" 다음에 "ip &lt;a.b.c.d&gt;" 줄이 이어진다. file은 spec 디렉터리 기준 상대 경로다.
 * 출력은 ASCII JSON이다(비ASCII 문자는 \\uXXXX). 예외는 class 이름과, 고정 문자열인 InvalidDatabaseException
 * message와 cause class만 남긴다. JDK마다 다른 message(arraycopy 등)는 기록하지 않는다.
 */
public class GeoipOracle {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: GeoipOracle <spec.txt> <out.json>");
        }
        File spec = new File(args[0]);
        List<String> lines = new ArrayList<String>();
        BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(spec), StandardCharsets.UTF_8));
        try {
            for (String line; (line = r.readLine()) != null; ) {
                if (line.length() > 0) {
                    lines.add(line);
                }
            }
        } finally {
            r.close();
        }

        StringBuilder out = new StringBuilder();
        out.append("{\n\"countryCodes\": [");
        List<String> codes = new LookupServiceCodes().codes();
        for (int i = 0; i < codes.size(); i++) {
            out.append(i == 0 ? "" : ", ").append(quote(codes.get(i)));
        }
        out.append("],\n\"databases\": [");
        boolean firstDb = true;
        boolean firstIp = true;
        LookupService ls = null;
        for (String line : lines) {
            if (line.startsWith("db ")) {
                String name = line.substring(3);
                if (!firstDb) {
                    out.append("\n]},");
                }
                firstDb = false;
                firstIp = true;
                out.append("\n{\"file\": ").append(quote(name)).append(", \"open\": ");
                ls = null;
                try {
                    ls = new LookupService(new File(spec.getParentFile(), name), LookupService.GEOIP_MEMORY_CACHE);
                    out.append(describeOpen(ls));
                } catch (Throwable t) {
                    out.append(describeThrowable(t));
                }
                out.append(",\n\"lookups\": [");
            } else if (line.startsWith("ip ")) {
                String ip = line.substring(3);
                out.append(firstIp ? "\n" : ",\n");
                firstIp = false;
                out.append("{\"ip\": ").append(quote(ip)).append(", ");
                if (ls == null) {
                    out.append("\"result\": \"not-open\"}");
                    continue;
                }
                try {
                    Location loc = ls.getLocation(InetAddress.getByAddress(parse(ip)));
                    if (loc == null) {
                        out.append("\"result\": \"null\"}");
                    } else {
                        out.append("\"result\": \"location\", \"countryCode\": ").append(quote(loc.countryCode))
                                .append(", \"region\": ").append(quote(loc.region))
                                .append(", \"city\": ").append(quote(loc.city))
                                .append(", \"postalCode\": ").append(quote(loc.postalCode))
                                .append(", \"latitudeBits\": ").append(quote(hex(Float.floatToIntBits(loc.latitude))))
                                .append(", \"longitudeBits\": ").append(quote(hex(Float.floatToIntBits(loc.longitude))))
                                .append(", \"metroCode\": ").append(loc.metro_code)
                                .append(", \"dmaCode\": ").append(loc.dma_code)
                                .append(", \"areaCode\": ").append(loc.area_code).append("}");
                    }
                } catch (Throwable t) {
                    out.append("\"result\": \"exception\", \"exception\": ").append(describeThrowable(t)).append("}");
                }
            } else {
                throw new IllegalArgumentException("bad spec line: " + line);
            }
        }
        if (!firstDb) {
            out.append("\n]}");
        }
        out.append("\n]\n}\n");
        Writer w = new OutputStreamWriter(new FileOutputStream(args[1]), StandardCharsets.US_ASCII);
        try {
            w.write(out.toString());
        } finally {
            w.close();
        }
    }

    /** getAllCountryCodes()는 public API다. 순서·길이를 그대로 기록한다. */
    static final class LookupServiceCodes {
        List<String> codes() throws Exception {
            File tmp = File.createTempFile("geoip-oracle-", ".dat");
            try {
                // 생성자에는 열 수 있는 파일이 필요하다. Country 형식 최소 파일(trie 1 node + FF FF FF 01)을 쓴다.
                byte[] b = new byte[200];
                b[b.length - 4] = (byte) 0xff;
                b[b.length - 3] = (byte) 0xff;
                b[b.length - 2] = (byte) 0xff;
                b[b.length - 1] = 1;
                FileOutputStream o = new FileOutputStream(tmp);
                try {
                    o.write(b);
                } finally {
                    o.close();
                }
                return new LookupService(tmp, LookupService.GEOIP_MEMORY_CACHE).getAllCountryCodes();
            } finally {
                tmp.delete();
            }
        }
    }

    static String describeOpen(LookupService ls) throws Exception {
        Field type = LookupService.class.getDeclaredField("databaseType");
        Field segments = LookupService.class.getDeclaredField("databaseSegments");
        Field recordLength = LookupService.class.getDeclaredField("recordLength");
        Field info = com.maxmind.geoip.DatabaseInfo.class.getDeclaredField("info");
        type.setAccessible(true);
        segments.setAccessible(true);
        recordLength.setAccessible(true);
        info.setAccessible(true);
        int[] seg = (int[]) segments.get(ls);
        return "{\"ok\": true, \"type\": " + type.getByte(ls)
                + ", \"segments\": " + (seg == null ? "null" : String.valueOf(seg[0]))
                + ", \"recordLength\": " + recordLength.getInt(ls)
                + ", \"info\": " + quote((String) info.get(ls.getDatabaseInfo())) + "}";
    }

    static String describeThrowable(Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"ok\": false, \"class\": ").append(quote(t.getClass().getName()));
        if (t instanceof com.maxmind.geoip.InvalidDatabaseException) {
            sb.append(", \"message\": ").append(quote(t.getMessage()));
            sb.append(", \"cause\": ").append(t.getCause() == null ? "null" : quote(t.getCause().getClass().getName()));
        }
        return sb.append("}").toString();
    }

    static byte[] parse(String ip) {
        String[] p = ip.split("\\.");
        if (p.length != 4) {
            throw new IllegalArgumentException(ip);
        }
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            int v = Integer.parseInt(p[i]);
            if (v < 0 || v > 255) {
                throw new IllegalArgumentException(ip);
            }
            b[i] = (byte) v;
        }
        return b;
    }

    static String hex(int v) {
        String s = Integer.toHexString(v);
        while (s.length() < 8) {
            s = "0" + s;
        }
        return s;
    }

    static String quote(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\').append(c);
            } else if (c < 0x20 || c > 0x7e) {
                String h = Integer.toHexString(c);
                sb.append("\\u");
                for (int k = h.length(); k < 4; k++) {
                    sb.append('0');
                }
                sb.append(h);
            } else {
                sb.append(c);
            }
        }
        return sb.append('"').toString();
    }
}

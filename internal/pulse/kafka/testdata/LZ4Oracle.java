// Java oracle for internal/pulse/kafka LZ4 compression (TestJavaLZ4Oracle).
// It runs only against the Pulse plugin's client, kafka-clients 4.0.0 (with
// lz4-java), from a local jar cache. Nothing here ships with the server.
//
//   decode <dir>  Reads every *.batch (one magic-2 RecordBatch written by Go)
//                 with MemoryRecords, validates CRC, decompresses with the Java
//                 LZ4 codec and prints one JSON line per batch. Every *.frame
//                 (a bare LZ4 frame) is decompressed through
//                 Compression.lz4().wrapForInput and printed as hex.
//   encode <dir>  Writes Java LZ4 frames (Compression.lz4().wrapForOutput) of
//                 fixed inputs as <name>.in/<name>.jframe, one Java lz4 record
//                 batch as java.batch, and lz4-java XXH32 vectors as xxh32.txt.

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.*;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import net.jpountz.xxhash.XXHashFactory;

public class LZ4Oracle {
    static String hex(ByteBuffer b) {
        if (b == null) return null;
        b = b.duplicate();
        StringBuilder sb = new StringBuilder(b.remaining() * 2);
        while (b.hasRemaining()) sb.append(String.format("%02x", b.get() & 0xff));
        return sb.toString();
    }
    static String hex(byte[] b) { return b == null ? null : hex(ByteBuffer.wrap(b)); }
    static String q(String s) { return s == null ? "null" : "\"" + s + "\""; }

    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(args[1]);
        if (args[0].equals("decode")) decode(dir);
        else if (args[0].equals("encode")) encode(dir);
        else throw new IllegalArgumentException(args[0]);
    }

    static void decode(Path dir) throws Exception {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) { for (Path p : ds) files.add(p); }
        Collections.sort(files);
        for (Path p : files) {
            String name = p.getFileName().toString();
            byte[] data = Files.readAllBytes(p);
            if (name.endsWith(".batch")) {
                MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(data));
                for (RecordBatch batch : records.batches()) {
                    batch.ensureValid();
                    StringBuilder sb = new StringBuilder();
                    sb.append("{\"file\":").append(q(name))
                      .append(",\"compression\":").append(q(batch.compressionType().name))
                      .append(",\"count\":").append(batch.countOrNull())
                      .append(",\"records\":[");
                    boolean first = true;
                    for (org.apache.kafka.common.record.Record r : batch) {
                        r.ensureValid();
                        if (!first) sb.append(',');
                        first = false;
                        sb.append("{\"offset\":").append(r.offset())
                          .append(",\"timestamp\":").append(r.timestamp())
                          .append(",\"key\":").append(q(hex(r.key())))
                          .append(",\"value\":").append(q(hex(r.value())))
                          .append(",\"headers\":[");
                        Header[] hs = r.headers();
                        for (int i = 0; i < hs.length; i++) {
                            if (i > 0) sb.append(',');
                            sb.append("{\"key\":").append(q(hex(hs[i].key().getBytes(StandardCharsets.UTF_8))))
                              .append(",\"value\":").append(q(hex(hs[i].value()))).append('}');
                        }
                        sb.append("]}");
                    }
                    sb.append("]}");
                    System.out.println(sb);
                }
            } else if (name.endsWith(".frame")) {
                Compression lz4 = Compression.lz4().build();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (InputStream in = lz4.wrapForInput(ByteBuffer.wrap(data), RecordBatch.MAGIC_VALUE_V2, BufferSupplier.NO_CACHING)) {
                    byte[] buf = new byte[8192];
                    for (int n; (n = in.read(buf)) > 0; ) out.write(buf, 0, n);
                }
                System.out.println("{\"file\":" + q(name) + ",\"plain\":" + q(hex(out.toByteArray())) + "}");
            }
        }
    }

    static byte[] javaFrame(byte[] in) throws IOException {
        ByteBufferOutputStream bout = new ByteBufferOutputStream(64);
        try (OutputStream out = Compression.lz4().build().wrapForOutput(bout, RecordBatch.MAGIC_VALUE_V2)) {
            out.write(in);
        }
        ByteBuffer b = bout.buffer();
        b.flip();
        byte[] res = new byte[b.remaining()];
        b.get(res);
        return res;
    }

    static void encode(Path dir) throws Exception {
        Map<String, byte[]> inputs = new LinkedHashMap<>();
        inputs.put("empty", new byte[0]);
        inputs.put("hello", "hello".getBytes(StandardCharsets.UTF_8));
        StringBuilder json = new StringBuilder();
        for (int i = 0; i < 3000; i++) json.append("{\"traceId\":").append(i).append(",\"service\":\"/api/v1/orders\"},");
        inputs.put("json", json.toString().getBytes(StandardCharsets.UTF_8));
        byte[] rnd = new byte[100_000];
        new Random(5).nextBytes(rnd);
        inputs.put("random", rnd);
        for (Map.Entry<String, byte[]> e : inputs.entrySet()) {
            Files.write(dir.resolve(e.getKey() + ".in"), e.getValue());
            Files.write(dir.resolve(e.getKey() + ".jframe"), javaFrame(e.getValue()));
        }
        ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, Compression.lz4().build(), TimestampType.CREATE_TIME, 0L);
        for (int i = 0; i < 20; i++) {
            byte[] key = ByteBuffer.allocate(8).putLong(-1234567890123L + i).array();
            byte[] value = ("{\"traceId\":" + i + ",\"text\":\"한글 repeated repeated\"}").getBytes(StandardCharsets.UTF_8);
            Header[] headers = {new RecordHeader("apm_type", "SCOUTER".getBytes(StandardCharsets.UTF_8)), new RecordHeader("null_value", null)};
            builder.append(1000L + i, key, i == 3 ? null : value, headers);
        }
        MemoryRecords built = builder.build();
        ByteBuffer bb = built.buffer();
        byte[] batch = new byte[bb.remaining()];
        bb.get(batch);
        Files.write(dir.resolve("java.batch"), batch);
        StringBuilder xx = new StringBuilder();
        String[] vectors = {"", "a", "abc", "Nobody inspects the spammish repetition", "0123456789abcdef0123456789abcdef!"};
        for (String v : vectors) {
            byte[] b = v.getBytes(StandardCharsets.UTF_8);
            xx.append(hex(b)).append(' ').append(Integer.toUnsignedString(XXHashFactory.fastestJavaInstance().hash32().hash(b, 0, b.length, 0))).append('\n');
        }
        byte[] fd = {0x60, 0x40};
        xx.append(hex(fd)).append(' ').append(Integer.toUnsignedString(XXHashFactory.fastestJavaInstance().hash32().hash(fd, 0, 2, 0))).append('\n');
        Files.write(dir.resolve("xxh32.txt"), xx.toString().getBytes(StandardCharsets.UTF_8));
    }
}

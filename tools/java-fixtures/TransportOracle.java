/* Scouter transport source: Copyright 2015 the original author or authors,
 * https://github.com/scouter-project/scouter, Apache License 2.0.
 * Synthetic fixtures independently assembled using the pinned DataOutputX. */
import java.nio.file.Files;
import java.nio.file.Path;
import scouter.io.DataOutputX;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.TextPack;
import scouter.net.NetCafe;
import scouter.net.TcpFlag;

public final class TransportOracle {
    private static Path output;

    private static void save(String name, DataOutputX bytes) throws Exception {
        Files.write(output.resolve("transport-" + name + ".bin"), bytes.toByteArray());
    }

    public static void main(String[] args) throws Exception {
        output = Path.of(args[0]);
        Files.createDirectories(output);
        TextPack text = new TextPack();
        text.xtype = "service";
        text.hash = -123;
        text.text = "synthetic transport 한글";
        MapPack map = new MapPack();
        map.put("x", -7);
        byte[] packet = new DataOutputX().writePack(text).toByteArray();
        for (String magic : new String[]{"CAFE", "JAVA"}) {
            save("udp-" + magic.toLowerCase(), new DataOutputX().write(magic.getBytes("US-ASCII")).write(packet));
        }
        for (String magic : new String[]{"CAFN", "JAVN"}) {
            save("udp-" + magic.toLowerCase(), new DataOutputX().write(magic.getBytes("US-ASCII"))
                    .writeShort(2).write(packet).writePack(map));
        }
        for (String magic : new String[]{"CAFM", "JMTU"}) {
            int midpoint = packet.length / 2;
            for (int index = 0; index < 2; index++) {
                byte[] part = java.util.Arrays.copyOfRange(packet, index == 0 ? 0 : midpoint,
                        index == 0 ? midpoint : packet.length);
                save("udp-" + magic.toLowerCase() + "-" + index,
                        new DataOutputX().write(magic.getBytes("US-ASCII"))
                        .writeInt(-7).writeLong(0x0102030405060708L)
                        .writeShort(2).writeShort(index).writeBlob(part));
            }
        }
        save("tcp-client", new DataOutputX().writeInt(NetCafe.TCP_CLIENT)
                .writeText("LOGIN").writeLong(0).writePack(map).writeText("CLOSE"));
        save("tcp-client-latch", new DataOutputX().writeInt(NetCafe.TCP_CLIENT)
                .writeText("COUNTER_REAL_TIME").writeLong(42).writePack(map)
                .writeText("COUNTER_REAL_TIME").writeLong(999).writePack(map).writeText("CLOSE"));
        for (int version = 1; version <= 2; version++) {
            save("tcp-agent-v" + version, new DataOutputX()
                    .writeInt(version == 1 ? NetCafe.TCP_AGENT : NetCafe.TCP_AGENT_V2).writeInt(-7));
            byte[] command = new DataOutputX().writeText("KEEP_ALIVE").writePack(map).toByteArray();
            save("tcp-agent-command-v" + version, version == 1 ? new DataOutputX().write(command)
                    : new DataOutputX().writeIntBytes(command));
            DataOutputX response = new DataOutputX().writeByte(TcpFlag.HasNEXT);
            if (version == 1) response.writePack(map);
            else response.writeIntBytes(new DataOutputX().writePack(map).toByteArray());
            save("tcp-agent-response-v" + version, response.writeByte(TcpFlag.NoNEXT));
        }
        byte[] zip = new byte[]{'P', 'K', 3, 4, 0, 1, 2, 3};
        save("tcp-stack", new DataOutputX().writeInt(NetCafe.TCP_AGENT_REQ).writeInt(-7)
                .writeInt(NetCafe.TCP_SEND_STACK).writeLong(1700000000123L)
                .writeText("/synthetic/app").writeText("stack.zip")
                .writeLong(zip.length).write(zip));
        save("tcp-stack-ok", new DataOutputX().writeByte(TcpFlag.OK));
    }
}

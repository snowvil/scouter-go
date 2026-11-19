package scouter.agent;

import scouter.net.NetConstants;

/**
 * Seam for the original scouter.agent.Configure (agent conf file, reload thread, obj name detection).
 *
 * Only the fields that the original DataProxy, UDPDataSendThread and DataUdpAgent read are kept.
 * Every default below is the original default; tools/java-loadgen/run.py checks each against
 * the getInt/getLong/getBoolean default in the pinned original Configure.java before compiling.
 * The load generator changes only the values recorded in its report (agent_conf).
 *
 * One intended difference: getObjHash() returns the object hash bound to the calling thread,
 * so one JVM can stand in for several agent objects (the original has one object per JVM).
 */
public class Configure {
    private static final Configure instance = new Configure();

    public static final Configure getInstance() {
        return instance;
    }

    public String net_local_udp_ip = null;
    public int net_local_udp_port = 0;
    public String net_collector_ip = "127.0.0.1";
    public int net_collector_udp_port = NetConstants.SERVER_UDP_PORT;
    public int net_udp_packet_max_bytes = 60000;
    public long net_udp_collection_interval_ms = 100;
    public int profile_step_max_count = 1024;
    public boolean ignore_global_consequent_sampling = false;
    public boolean _log_udp_xlog_enabled = false;
    public boolean _log_udp_object_enabled = false;
    public boolean _log_udp_counter_enabled = false;
    public String obj_type = "java";

    private static final ThreadLocal<Integer> objHash = ThreadLocal.withInitial(() -> 0);

    public static void bindObjHash(int hash) {
        objHash.set(hash);
    }

    public int getObjHash() {
        return objHash.get();
    }
}

// Seam: the original Configure reads scouter.conf with a reload thread. The oracle sets the fields
// AgentManager reads (object_*); log_index_traversal_warning_count keeps the index-oracle stub field
// that the common db/io classes were compiled against.
package scouter.server;
public final class Configure {
    private static final Configure instance = new Configure();
    public int log_index_traversal_warning_count = 100;
    public int object_deadtime_ms = 8000;
    public int object_zipkin_deadtime_ms = 300000;
    public int object_inactive_alert_level = 0;
    public String log_udp_some_object = "";
    public static Configure getInstance() { return instance; }
}

// Seam: alert pipeline sink (PluginHelper.alert* only; never invoked by descriptor cases).
package scouter.server.core;
import java.util.ArrayList;
import java.util.List;
public final class AlertCore { public static final List<scouter.lang.pack.AlertPack> added = new ArrayList<scouter.lang.pack.AlertPack>(); public static void add(scouter.lang.pack.AlertPack p) { added.add(p); } }

package scouter.server;

import scouter.util.StringSet;

/**
 * Seam for the pinned scouter.server.Configure: only the fields read by the original
 * Logger.scala and RequestLogger.java, with the original defaults (Configure.java:185-191,
 * 266, 341). The oracle mutates the fields directly and runs ConfObserver.exec() the way
 * Configure.applyConfig does after a reload.
 */
public class Configure {
	private static Configure instance;

	public static synchronized Configure getInstance() {
		if (instance == null)
			instance = new Configure();
		return instance;
	}

	public boolean log_rotation_enabled = true;
	public int log_keep_days = 31;
	public boolean _trace = false;
	public String log_dir = "./logs";
	public StringSet mgr_log_ignore_ids = new StringSet();
}

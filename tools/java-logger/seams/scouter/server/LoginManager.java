package scouter.server;

import java.util.HashMap;
import java.util.Map;

/**
 * Seam for the pinned scouter.server.LoginManager: RequestLogger only calls getUser(session),
 * which returns null for an unknown session (LoginManager.scala getUser).
 */
public class LoginManager {
	private static final Map<Long, LoginUser> users = new HashMap<Long, LoginUser>();

	public static synchronized void register(long session, String id, String ip) {
		LoginUser u = new LoginUser();
		u.session_$eq(session);
		u.id_$eq(id);
		u.ip_$eq(ip);
		users.put(session, u);
	}

	public static synchronized LoginUser getUser(long session) {
		return users.get(session);
	}
}

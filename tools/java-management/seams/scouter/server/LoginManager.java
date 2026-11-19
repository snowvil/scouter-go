// Seam: original LoginManager authenticates through AccountManager/ServiceWorker and keeps a CacheTable.
// Cases seed synthetic sessions explicitly; getUser/getLoginUserList keep the original semantics (null when unknown).
// login keeps the original body (LoginManager.scala:35-65: AccountManager.authorizeAccount, LoginUser fields,
// internal flag) but issues a deterministic session counter instead of KeyGen.next(); the __local branch
// (ServiceWorker remote IP) is not exercised. validSession is containsKey (no CacheTable expiry).
package scouter.server;
import java.util.LinkedHashMap;
import java.util.Map;
import scouter.lang.Account;
import scouter.server.account.AccountManager;
public final class LoginManager {
    private static final Map<Long, LoginUser> sessions = new LinkedHashMap<Long, LoginUser>();
    private static long nextSession = 0x5e55100000L;
    public static synchronized LoginUser addUser(long session, String id, String ip, String group) {
        LoginUser u = new LoginUser(); u.session_$eq(session); u.id_$eq(id); u.ip_$eq(ip); u.group_$eq(group); u.logintime_$eq(0L);
        sessions.put(session, u); return u;
    }
    public static synchronized long login(String id, String pwd, String ip, boolean internal) {
        Account account = AccountManager.authorizeAccount(id, pwd);
        if (account == null) return 0L;
        LoginUser u = addUser(++nextSession, id, ip, account.group);
        u.passwd_$eq(pwd); u.internal_$eq(internal);
        return u.session();
    }
    public static synchronized int validSession(long key) { return sessions.containsKey(key) ? 1 : 0; }
    public static synchronized LoginUser getUser(long session) { return sessions.get(session); }
    public static synchronized LoginUser[] getLoginUserList() { return sessions.values().toArray(new LoginUser[0]); }
    public static synchronized void clear() { sessions.clear(); }
}

package scouterx.webapp.framework.client.model;

// Oracle stub: dictionary lookups return "<type>#<hash>" (0 -> "").
public class TextProxy {
    public static class Model {
        final String type;
        Model(String type) { this.type = type; }
        public String getCachedTextIfNullDefault(int hash) {
            if (hash == 0) return "";
            return type + "#" + hash;
        }
    }
    public static final Model service = new Model("service");
    public static final Model hashMessage = new Model("hmsg");
    public static final Model error = new Model("error");
    public static final Model userAgent = new Model("ua");
    public static final Model referrer = new Model("referer");
    public static final Model group = new Model("group");
    public static final Model city = new Model("city");
    public static final Model login = new Model("login");
    public static final Model desc = new Model("desc");
}

// Executes the unchanged Apache-2.0 Scouter 2.21.3 management handlers (AccountService, ConfigureService,
// AlertScriptingService, ManageService, AgentInfo, ThreadList) through the original ServiceHandlingProxy,
// with the original AccountManager/AccountFileHandler/GroupFileHandler/Configure/CounterManager/
// RemoteControlManager/AlertRuleLoader/RealCounter/PluginHelper. Infrastructure seams are listed in
// tools/java-management/README.md. One JVM per scenario; the working directory is a fresh temp dir.
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import scouter.io.DataInputX;
import scouter.io.DataOutputX;
import scouter.lang.Account;
import scouter.lang.counters.CounterEngine;
import scouter.lang.pack.MapPack;
import scouter.lang.pack.ObjectPack;
import scouter.lang.pack.Pack;
import scouter.lang.value.BlobValue;
import scouter.lang.value.FloatValue;
import scouter.lang.value.ListValue;
import scouter.lang.value.MapValue;
import scouter.lang.value.TextValue;
import scouter.lang.value.Value;
import scouter.lang.value.ValueEnum;
import scouter.net.RequestCmd;
import scouter.net.TcpFlag;
import scouter.server.Configure;
import scouter.server.CounterManager;
import scouter.server.LoginManager;
import scouter.server.account.AccountFileHandler;
import scouter.server.account.GroupFileHandler;
import scouter.server.core.AgentManager;
import scouter.server.core.cache.AlertScriptLoadMessageCache;
import scouter.server.db.ObjectRD;
import scouter.server.db.ObjectWR;
import scouter.util.CipherUtil;
import scouter.util.SysJMX;

public final class ManagementOracle extends OracleSupport {
    public static void main(String[] args) throws Exception {
        String scenario = args[0];
        out = Paths.get(args[1]);
        // Stops ThreadScala daemons (AccountManager 5s reload, RemoteControlManager 5s session sweep) before
        // their objects initialize, so queue/map state only changes through the handler under test.
        scouter.server.core.CoreRun.running = false;
        switch (scenario) {
            case "account": account(); break;
            case "configure": configure(); break;
            case "counters": counters(); break;
            case "alert": alert(); break;
            case "manage": manage(); break;
            case "agentinfo": agentinfo(); break;
            case "threadlist": threadlist(); break;
            case "login": LiveConfOracle.login(); break;
            case "xlogbounds": LiveConfOracle.xlogBounds(); break;
            case "verify-xml": verifyXml(out); return;
            case "descriptors": descriptors(); return;
            default: TelegrafOracle.main(args); return;
        }
        finish(scenario);
        System.exit(0); // daemon threads (Configure, AlertRuleLoader, AsyncRun) never finish on their own
    }

    static Path conf(String name) { return CWD.resolve("conf").resolve(name); }

    static void writeConf(String name, String text) throws Exception {
        Path p = conf(name);
        Files.createDirectories(p.getParent());
        Files.write(p, text.getBytes(StandardCharsets.UTF_8));
    }

    static Map<String, Object> fileFact(Path p) throws Exception {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("exists", Files.exists(p));
        if (Files.exists(p)) { byte[] b = read(p); m.put("bytes", b.length); m.put("sha256", sha256(b)); }
        return m;
    }

    // ---------------------------------------------------------------- account
    static final String PASS1 = CipherUtil.sha256("synthetic-password-1");
    static final String PASS2 = CipherUtil.sha256("synthetic-password-2");

    static void account() throws Exception {
        // AccountManager copies the original classpath defaults into ./conf on first use.
        decorateAccounts(dispatch("list-account-initial", RequestCmd.LIST_ACCOUNT, NONE, 'V'));
        Path acc = conf("account.xml"), grp = conf("account_group.xml");
        byte[] accBefore = read(acc), grpBefore = read(grp);
        write("account.default.xml", accBefore);
        write("account_group.default.xml", grpBefore);
        facts.put("synthetic_password_digest_1", PASS1);
        facts.put("synthetic_password_digest_2", PASS2);
        dispatch("list-account-group-initial", RequestCmd.LIST_ACCOUNT_GROUP, NONE, 'P');
        dispatch("group-policy-all-initial", RequestCmd.GET_GROUP_POLICY_ALL, NONE, 'P');
        dispatch("check-account-id-admin", RequestCmd.CHECK_ACCOUNT_ID, pack(map("id", "admin")), 'P');
        dispatch("check-account-id-free", RequestCmd.CHECK_ACCOUNT_ID, pack(map("id", "u1")), 'P');

        dispatch("add-account-u1", RequestCmd.ADD_ACCOUNT, pack(map("id", "u1", "pass", PASS1, "email", "u1@example.invalid", "group", "guest")), 'P');
        dispatch("add-account-duplicate", RequestCmd.ADD_ACCOUNT, pack(map("id", "u1", "pass", PASS2, "email", "dup@example.invalid", "group", "admin")), 'P');
        dispatch("add-account-unicode", RequestCmd.ADD_ACCOUNT, pack(map("id", "사용자", "pass", PASS2, "email", "유니코드@example.invalid", "group", "guest")), 'P');
        dispatch("add-account-no-id", RequestCmd.ADD_ACCOUNT, pack(map("pass", PASS1, "email", "x@example.invalid", "group", "guest")), 'P');
        dispatch("add-account-not-mappack", RequestCmd.ADD_ACCOUNT, value(new TextValue("u9")), 'P');
        dispatch("edit-account-u1", RequestCmd.EDIT_ACCOUNT, pack(map("id", "u1", "pass", PASS2, "email", "u1-edit@example.invalid", "group", "admin")), 'P');
        dispatch("edit-account-missing", RequestCmd.EDIT_ACCOUNT, pack(map("id", "nobody", "pass", PASS2, "email", "n@example.invalid", "group", "guest")), 'P');
        decorateAccounts(dispatch("list-account-after-edit", RequestCmd.LIST_ACCOUNT, NONE, 'V'));
        facts.put("account_xml_unchanged_after_add_edit", java.util.Arrays.equals(accBefore, read(acc)));

        dispatch("add-group", RequestCmd.ADD_ACCOUNT_GROUP, pack(map("name", "synthetic-group", "policy",
                mapValue("AllowHeapDump", true, "AllowExportCounter", false, "CustomTextPolicy", "yes"))), 'V');
        dispatch("add-group-no-name", RequestCmd.ADD_ACCOUNT_GROUP, pack(map("policy", mapValue("AllowHeapDump", true))), 'V');
        dispatch("add-group-no-policy", RequestCmd.ADD_ACCOUNT_GROUP, pack(map("name", "g2")), 'V');
        dispatch("add-group-policy-not-map", RequestCmd.ADD_ACCOUNT_GROUP, pack(map("name", "g3", "policy", "AllowHeapDump")), 'V');
        dispatch("list-account-group-after-add", RequestCmd.LIST_ACCOUNT_GROUP, NONE, 'P');
        dispatch("group-policy-all-after-add", RequestCmd.GET_GROUP_POLICY_ALL, NONE, 'P');
        dispatch("edit-group-policy-guest", RequestCmd.EDIT_GROUP_POLICY, pack(map("guest",
                mapValue("AllowHeapDump", true, "AllowNewPolicy", true))), 'V');
        dispatch("group-policy-all-after-edit", RequestCmd.GET_GROUP_POLICY_ALL, NONE, 'P');
        dispatch("edit-group-policy-unknown-group", RequestCmd.EDIT_GROUP_POLICY, pack(map("no-such-group", mapValue("AllowHeapDump", true))), 'V');
        dispatch("edit-group-policy-not-map", RequestCmd.EDIT_GROUP_POLICY, pack(map("guest", "AllowHeapDump")), 'V');
        facts.put("account_group_xml_unchanged", java.util.Arrays.equals(grpBefore, read(grp)));
        facts.put("account_xml_unchanged", java.util.Arrays.equals(accBefore, read(acc)));

        // null email: Account.email stays null in memory, so the next LIST_ACCOUNT hits toBytes NPE mid-stream.
        dispatch("add-account-no-email", RequestCmd.ADD_ACCOUNT, pack(map("id", "u-no-email", "pass", PASS1, "group", "guest")), 'P');
        decorateAccounts(dispatch("list-account-after-null-email", RequestCmd.LIST_ACCOUNT, NONE, 'V'));

        // Account.toBytes direct (UTF-8 platform charset, 1-byte length = low 8 bits of the byte count)
        List<Object> toBytes = new ArrayList<Object>();
        toBytes.add(accountBytes("account-tobytes-basic", "u1", PASS1, "u1@example.invalid", "guest"));
        toBytes.add(accountBytes("account-tobytes-unicode", "사용자", PASS2, "유니코드@example.invalid", "그룹"));
        toBytes.add(accountBytes("account-tobytes-empty", "", "", "", ""));
        StringBuilder longId = new StringBuilder(); for (int i = 0; i < 260; i++) longId.append('a');
        toBytes.add(accountBytes("account-tobytes-260", longId.toString(), PASS1, "e@example.invalid", "guest"));
        facts.put("account_tobytes", toBytes);

        // Original parsers on the unchanged defaults (what a restart would load).
        facts.put("parsed_default_accounts", describeAccounts(AccountFileHandler.parse(acc.toFile())));
        facts.put("parsed_default_groups", describeGroups(GroupFileHandler.parse(grp.toFile())));
    }

    /** Adds Account.toObject decoding of LIST_ACCOUNT Blob items (original common decoder). */
    @SuppressWarnings("unchecked")
    static void decorateAccounts(Map<String, Object> rec) throws Exception {
        List<Object> accounts = new ArrayList<Object>();
        for (Object item : (List<Object>) rec.get("items")) {
            Map<String, Object> v = (Map<String, Object>) item;
            if (!"blob".equals(v.get("t"))) { accounts.add(null); continue; }
            Account a = new Account();
            a.toObject(hexBytes((String) v.get("hex")));
            Map<String, Object> x = new LinkedHashMap<String, Object>();
            x.put("id", a.id); x.put("pass", a.password); x.put("email", a.email); x.put("group", a.group);
            accounts.add(x);
        }
        rec.put("accounts", accounts);
    }

    static byte[] hexBytes(String h) {
        byte[] b = new byte[h.length() / 2];
        for (int i = 0; i < b.length; i++) b[i] = (byte) Integer.parseInt(h.substring(i * 2, i * 2 + 2), 16);
        return b;
    }

    static Map<String, Object> accountBytes(String name, String id, String pass, String email, String group) throws Exception {
        Account a = new Account(); a.id = id; a.password = pass; a.email = email; a.group = group;
        byte[] b = a.toBytes();
        write(name + ".bin", b);
        Account back = new Account(); back.toObject(b);
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("name", name); m.put("file", name + ".bin"); m.put("bytes", b.length);
        m.put("roundtrip_equal", id.equals(back.id) && pass.equals(back.password) && email.equals(back.email) && group.equals(back.group));
        return m;
    }

    static List<Object> describeAccounts(scouter.util.StringKeyLinkedMap<Account> m) {
        List<Object> l = new ArrayList<Object>();
        scouter.util.StringEnumer keys = m.keys();
        while (keys.hasMoreElements()) {
            Account a = m.get(keys.nextString());
            Map<String, Object> x = new LinkedHashMap<String, Object>();
            x.put("id", a.id); x.put("pass", a.password); x.put("email", a.email); x.put("group", a.group);
            l.add(x);
        }
        return l;
    }

    static Map<String, Object> describeGroups(scouter.util.StringKeyLinkedMap<MapValue> m) {
        Map<String, Object> g = new LinkedHashMap<String, Object>();
        scouter.util.StringEnumer keys = m.keys();
        while (keys.hasMoreElements()) { String k = keys.nextString(); g.put(k, describeValue(m.get(k))); }
        return g;
    }

    // ---------------------------------------------------------------- configure
    static final String INITIAL_CONF = "server_id=synthetic-server\n# synthetic oracle config\nnet_http_port=6181\n";

    static void configure() throws Exception {
        writeConf("scouter.conf", INITIAL_CONF);
        String host = SysJMX.getHostName();
        dispatch("get-configure-server", RequestCmd.GET_CONFIGURE_SERVER, NONE, 'P');
        dispatch("list-configure-server-initial", RequestCmd.LIST_CONFIGURE_SERVER, NONE, 'P');
        redact("list-configure-server-initial", host);
        writeKeyValueTable("list-configure-server-initial", "key-value-table.initial.json");
        dispatch("configure-desc", RequestCmd.CONFIGURE_DESC, pack(map("objHash", 0)), 'P');
        dispatch("configure-value-type", RequestCmd.CONFIGURE_VALUE_TYPE, pack(map("objHash", 0)), 'P');
        dispatch("configure-value-type-desc", RequestCmd.CONFIGURE_VALUE_TYPE_DESC, pack(map("objHash", 0)), 'P');
        dispatch("configure-value-type-desc-agent", RequestCmd.CONFIGURE_VALUE_TYPE_DESC, pack(map("objHash", 42)), 'P');

        String newConf = "server_id=synthetic-server-2\n"
                + "xlog_queue_size=777\n"
                + "object_deadtime_ms = 9000 \n"
                + "net_http_api_auth_ip_header_key=${scouter.oracle.sysprop}\n"
                + "ext_link_name=prefix-${scouter.oracle.sysprop}-suffix\n"
                + "ext_link_url_pattern=open-${\n"
                + "mgr_purge_keep_days=7\n"
                + "log_keep_days=x-not-a-number\n"
                + "net_http_api_enabled=TRUE\n"
                + "unicode_comment_key=한글\n"
                + "escaped_key=line1\\\n    continued\n";
        dispatch("set-configure-server", RequestCmd.SET_CONFIGURE_SERVER, pack(map("setConfig", newConf)), 'P');
        write("scouter.conf.after-set", read(conf("scouter.conf")));
        dispatch("list-configure-server-after-set", RequestCmd.LIST_CONFIGURE_SERVER, NONE, 'P');
        redact("list-configure-server-after-set", host);
        Configure c = Configure.getInstance();
        Map<String, Object> applied = new LinkedHashMap<String, Object>();
        applied.put("server_id", c.server_id); applied.put("xlog_queue_size", c.xlog_queue_size);
        applied.put("object_deadtime_ms", c.object_deadtime_ms); applied.put("net_http_api_auth_ip_header_key", c.net_http_api_auth_ip_header_key);
        applied.put("ext_link_name", c.ext_link_name); applied.put("ext_link_url_pattern", c.ext_link_url_pattern);
        applied.put("mgr_purge_keep_days", c.mgr_purge_keep_days); applied.put("log_keep_days", c.log_keep_days);
        applied.put("net_http_api_enabled", c.net_http_api_enabled);
        applied.put("property_unicode_comment_key", c.getValue("unicode_comment_key"));
        applied.put("property_escaped_key", c.getValue("escaped_key"));
        facts.put("applied_after_set", applied);
        dispatch("get-configure-server-after-set", RequestCmd.GET_CONFIGURE_SERVER, NONE, 'P');
        dispatch("set-configure-server-null", RequestCmd.SET_CONFIGURE_SERVER, pack(map("other", "x")), 'P');
        facts.put("scouter_conf_after_null_set", fileFact(conf("scouter.conf")));

        // loadText/saveText direct (platform charset = UTF-8 under -Dfile.encoding=UTF-8)
        String unicode = "server_id=합성\nnet_http_port=6181\n";
        boolean saved = c.saveText(unicode);
        String loaded = c.loadText();
        Map<String, Object> io = new LinkedHashMap<String, Object>();
        io.put("save_result", saved); io.put("load_equals", unicode.equals(loaded));
        io.put("file_sha256", sha256(read(conf("scouter.conf")))); io.put("file_bytes", read(conf("scouter.conf")).length);
        facts.put("save_load_text_unicode", io);

        // Telegraf config file (./conf/scouter-telegraf.xml is the only path; input_telegraf_config_file is not applied)
        dispatch("get-configure-telegraf-sample", RequestCmd.GET_CONFIGURE_TELEGRAF, NONE, 'P');
        String tg = TelegrafOracle.CUSTOM_TG_XML;
        dispatch("set-configure-telegraf", RequestCmd.SET_CONFIGURE_TELEGRAF, pack(map("tgConfigContents", tg)), 'P');
        facts.put("telegraf_file_after_set", fileFact(conf("scouter-telegraf.xml")));
        Map<String, Object> tgApplied = new LinkedHashMap<String, Object>();
        tgApplied.put("input_telegraf_enabled", c.input_telegraf_enabled);
        tgApplied.put("net_http_server_enabled", c.net_http_server_enabled);
        tgApplied.put("input_telegraf_debug_enabled", c.input_telegraf_debug_enabled);
        tgApplied.put("telegraf_object_deadtime_ms", c.telegraf_object_deadtime_ms);
        tgApplied.put("measurements", new ArrayList<Object>(new java.util.TreeSet<String>(c.telegrafInputConfigMap.keySet())));
        facts.put("telegraf_applied_after_set", tgApplied);
        dispatch("get-configure-telegraf-after-set", RequestCmd.GET_CONFIGURE_TELEGRAF, NONE, 'P');
        dispatch("set-configure-telegraf-null", RequestCmd.SET_CONFIGURE_TELEGRAF, pack(map("other", "x")), 'P');
        dispatch("set-configure-telegraf-invalid-xml", RequestCmd.SET_CONFIGURE_TELEGRAF, pack(map("tgConfigContents", "<TelegrafConfig><enabled>")), 'P');
        dispatch("list-configure-server-after-telegraf", RequestCmd.LIST_CONFIGURE_SERVER, NONE, 'P');
        redact("list-configure-server-after-telegraf", host);
        facts.put("env_sysprop_dump_evidence", envDumps);
    }

    /** Joins LIST_CONFIGURE_SERVER key/value/default lists (after redaction) into one ordered table. */
    static void writeKeyValueTable(String responseName, String file) throws Exception {
        DataInputX in = new DataInputX(read(out.resolve(responseName + ".response.bin")));
        require(in.readByte() == TcpFlag.HasNEXT, "LIST_CONFIGURE_SERVER item");
        MapPack mp = (MapPack) in.readPack();
        ListValue k = mp.getList("key"), v = mp.getList("value"), d = mp.getList("default");
        require(k.size() == v.size() && k.size() == d.size(), "key/value/default lengths");
        List<Object> rows = new ArrayList<Object>();
        for (int i = 0; i < k.size(); i++) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("key", k.getString(i)); row.put("value", describeValue(v.get(i))); row.put("default", describeValue(d.get(i)));
            rows.add(row);
        }
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        root.put("source", responseName + ".response.bin");
        root.put("note", "default = field initializer of new Configure(true) (ConfigValueUtil.getConfigDefault), not the applyConfig fallback");
        root.put("rows", rows);
        writeText(file, Json.write(root) + "\n");
        facts.put("key_value_rows", rows.size());
    }

    static final Pattern IDENTITY = Pattern.compile("([A-Za-z_$][\\w$.]*)@[0-9a-f]{1,8}");
    static final List<Object> envDumps = new ArrayList<Object>();

    /**
     * Rewrites Text values that carry host- or JVM-identity-dependent strings: SysJMX.getHostName()
     * (Configure.server_id field initializer) and Object.toString identity hashes (e.g. TgConfig@1b2c3d).
     * The original bytes would otherwise make the golden machine/run dependent. Every rewrite is recorded.
     */
    static void redact(String name, String host) throws Exception {
        Path p = out.resolve(name + ".response.bin");
        DataInputX in = new DataInputX(read(p));
        DataOutputX o = new DataOutputX();
        List<Object> rewrites = new ArrayList<Object>();
        while (true) {
            byte flag = in.readByte();
            o.writeByte(flag);
            if (flag == TcpFlag.NoNEXT) break;
            MapPack mp = (MapPack) in.readPack();
            Iterator<String> keys = mp.keys();
            while (keys.hasNext()) { String k = keys.next(); mp.put(k, redactValue(mp.get(k), host, k, rewrites)); }
            o.writePack(mp);
        }
        Files.write(p, o.toByteArray());
        for (Object rec : cases) {
            @SuppressWarnings("unchecked") Map<String, Object> r = (Map<String, Object>) rec;
            if (name.equals(r.get("name"))) {
                r.put("response_bytes", read(p).length);
                r.put("items", decodeItems(read(p), 'P'));
                r.put("redactions", rewrites);
            }
        }
    }

    static Value redactValue(Value v, String host, String path, List<Object> rewrites) {
        if (v instanceof ListValue) {
            ListValue l = (ListValue) v;
            for (int i = 0; i < l.size(); i++) l.set(i, redactValue(l.get(i), host, path + "[" + i + "]", rewrites));
            return l;
        }
        if (v instanceof TextValue) {
            String s = ((TextValue) v).value;
            String r = s;
            if (host != null && host.length() > 0 && r != null && r.equals(host)) r = "<hostname>";
            if (r != null) {
                if (r.contains(ENV_MARKER)) {
                    Map<String, Object> e = new LinkedHashMap<String, Object>();
                    e.put("path", path); e.put("env_marker_present", true); e.put("contains_java_version_sysprop", r.contains("java.version="));
                    e.put("contains_user_dir_sysprop", r.contains("user.dir=")); e.put("dump_starts_with_brace", r.indexOf('{') >= 0);
                    envDumps.add(e);
                    r = scrubEnvDump(r);
                }
                Matcher m = IDENTITY.matcher(r); r = m.replaceAll("$1@<identity>");
            }
            if (r != null && !r.equals(s)) {
                Map<String, Object> w = new LinkedHashMap<String, Object>();
                w.put("path", path); w.put("replacement", r); rewrites.add(w);
                return new TextValue(r);
            }
        }
        return v;
    }

    // ---------------------------------------------------------------- counters.site.xml
    static void counters() throws Exception {
        writeConf("scouter.conf", INITIAL_CONF);
        LoginManager.addUser(5001L, "synthetic-a", "10.0.0.1", "admin");
        LoginManager.addUser(5002L, "synthetic-b", "10.0.0.2", "guest");
        Path site = conf("counters.site.xml");
        dispatch("get-xml-counter-initial", RequestCmd.GET_XML_COUNTER, NONE, 'P');
        dispatch("get-counters-site-initial", RequestCmd.GET_CONFIGURE_COUNTERS_SITE, NONE, 'P');
        dispatch("define-object-type", RequestCmd.DEFINE_OBJECT_TYPE, pack(map("name", "synthetic_type", "disp", "Synthetic Type",
                "family", "javaee", "icon", "tomcat", "sub-object", false)), 'V');
        snapshot(site, "counters.site.after-define.xml");
        dispatch("define-object-type-duplicate", RequestCmd.DEFINE_OBJECT_TYPE, pack(map("name", "synthetic_type", "disp", "Again",
                "family", "javaee", "icon", "tomcat", "sub-object", false)), 'V');
        dispatch("define-object-type-unknown-family", RequestCmd.DEFINE_OBJECT_TYPE, pack(map("name", "synthetic_type2", "disp", "X",
                "family", "no_such_family", "icon", "", "sub-object", true)), 'V');
        facts.put("site_after_unknown_family", fileFact(site));
        dispatch("edit-object-type", RequestCmd.EDIT_OBJECT_TYPE, pack(map("name", "synthetic_type", "disp", "Edited",
                "family", "javaee", "icon", "redis", "sub-object", true)), 'V');
        facts.put("site_unchanged_after_edit", java.util.Arrays.equals(read(site), read(out.resolve("counters.site.after-define.xml"))));
        facts.put("engine_after_edit", describeObjectType("synthetic_type"));
        dispatch("get-xml-counter-after-define", RequestCmd.GET_XML_COUNTER, NONE, 'P');
        dispatch("get-counters-site-after-define", RequestCmd.GET_CONFIGURE_COUNTERS_SITE, NONE, 'P');

        String input = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Counters>\n"
                + "  <Familys>\n"
                + "    <Family name=\"myfam\" master=\"c1\" custom-attr=\"dropped\">\n"
                + "      <Counter name=\"c1\" disp=\"C1\" unit=\"cnt\" total=\"true\"/>\n"
                + "      <Counter name=\"c2\" disp=\"C2\" unit=\"ms\" all=\"false\" icon=\"i\"/>\n"
                + "    </Family>\n"
                + "    <Family name=\"myfam\" master=\"c3\"><Counter name=\"c3\" disp=\"C3\" unit=\"\"/></Family>\n"
                + "    <Family name=\"zws-metric.fam\" master=\"z\"><Counter name=\"z\"/></Family>\n"
                + "    <Family name=\"afam\" master=\"a1\"><Counter name=\"a1\"/></Family>\n"
                + "  </Familys>\n"
                + "  <Types>\n"
                + "    <ObjectType name=\"t_b\" family=\"myfam\" disp=\"TB\" icon=\"x\"><Counter name=\"own\" disp=\"Own\"/></ObjectType>\n"
                + "    <ObjectType name=\"t_a\" family=\"afam\" sub-object=\"true\"/>\n"
                + "    <ObjectType name=\"t_b\" family=\"myfam\" disp=\"TB2\"/>\n"
                + "    <ObjectType name=\"한글\" family=\"afam\" disp=\"한\"/>\n"
                + "  </Types>\n</Counters>\n";
        dispatch("set-counters-site", RequestCmd.SET_CONFIGURE_COUNTERS_SITE, pack(map("contents", input)), 'P');
        snapshot(site, "counters.site.after-set.xml");
        writeText("counters.site.set-input.xml", input);
        waitForQueue(5001L);
        dispatch("check-job-after-set-5001", RequestCmd.CHECK_JOB, pack(map("session", 5001L)), 'P');
        dispatch("check-job-after-set-5002", RequestCmd.CHECK_JOB, pack(map("session", 5002L)), 'P');
        facts.put("engine_t_b_after_set", describeObjectType("t_b"));
        dispatch("set-counters-site-invalid-xml", RequestCmd.SET_CONFIGURE_COUNTERS_SITE, pack(map("contents", "<Counters><Types>")), 'P');
        dispatch("set-counters-site-types-empty", RequestCmd.SET_CONFIGURE_COUNTERS_SITE, pack(map("contents", "<Counters><Types/></Counters>")), 'P');
        facts.put("site_after_types_empty", fileFact(site));
        dispatch("set-counters-site-no-contents", RequestCmd.SET_CONFIGURE_COUNTERS_SITE, pack(map("other", "x")), 'P');
        dispatch("set-counters-site-empty-root", RequestCmd.SET_CONFIGURE_COUNTERS_SITE, pack(map("contents", "<Counters/>")), 'P');
        snapshot(site, "counters.site.after-empty-root.xml");
        waitForQueue(5001L);
        // CounterEngine.parse acceptance of each written file (what reloadEngine/Client GET_XML_COUNTER would parse)
        Map<String, Object> parse = new LinkedHashMap<String, Object>();
        for (String f : new String[] {"counters.site.after-define.xml", "counters.site.after-set.xml", "counters.site.after-empty-root.xml"})
            parse.put(f, new CounterEngine().parse(read(out.resolve(f))));
        facts.put("counter_engine_parse", parse);
    }

    static void snapshot(Path src, String name) throws Exception { if (Files.exists(src)) write(name, read(src)); else facts.put(name, "missing"); }

    static void waitForQueue(long session) {
        // RegisterHandler.notifyAllClients enqueues through the original AsyncRun thread.
        for (int i = 0; i < 100; i++) {
            if (scouter.server.management.RemoteControlManager.commandTable().get(session) != null) { sleep(200); return; }
            sleep(50);
        }
    }

    static Map<String, Object> describeObjectType(String name) {
        scouter.lang.ObjectType t = CounterManager.getInstance().getCounterEngine().getObjectType(name);
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        if (t == null) { m.put("exists", false); return m; }
        m.put("exists", true); m.put("name", t.getName()); m.put("disp", t.getDisplayName()); m.put("icon", t.getIcon());
        m.put("family", t.getFamily() == null ? null : t.getFamily().getName()); m.put("subObject", t.isSubObject());
        List<Object> counters = new ArrayList<Object>();
        for (scouter.lang.Counter c : t.listObjectTypeCounters()) counters.add(c.getName());
        m.put("objectTypeCounters", counters);
        return m;
    }

    // ---------------------------------------------------------------- alert scripting
    static void alert() throws Exception {
        writeConf("scouter.conf", INITIAL_CONF + "plugin_dir=./plugin\n");
        // Load-message ring first, before AlertRuleLoader starts (its 5s loop would add compile messages).
        dispatch("load-message-empty", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 0L, "index", 0)), 'P');
        for (int i = 1; i <= 3; i++) AlertScriptLoadMessageCache.put("synthetic message " + i);
        dispatch("load-message-3-from-0", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 0L, "index", 0)), 'P');
        dispatch("load-message-3-from-2", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 0L, "index", 2)), 'P');
        dispatch("load-message-3-from-3", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 0L, "index", 3)), 'P');
        for (int i = 4; i <= 52; i++) AlertScriptLoadMessageCache.put("synthetic message " + i);
        // now loop=1, index=2 (52 puts into a 50-slot ring)
        dispatch("load-message-loop1-from-0-3", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 0L, "index", 3)), 'P');
        dispatch("load-message-loop1-from-0-1", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 0L, "index", 1)), 'P');
        dispatch("load-message-loop1-current", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 1L, "index", 2)), 'P');
        dispatch("load-message-loop1-from-1-0", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 1L, "index", 0)), 'P');
        for (int i = 53; i <= 160; i++) AlertScriptLoadMessageCache.put("synthetic message " + i);
        dispatch("load-message-loop3-stale", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 0L, "index", 0)), 'P');
        dispatch("load-message-future-loop", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", 9L, "index", 0)), 'P');
        dispatch("load-message-text-cursor", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, pack(map("loop", "3", "index", "10")), 'P');
        dispatch("load-message-no-body", RequestCmd.GET_ALERT_SCRIPT_LOAD_MESSAGE, NONE, 'P');

        Map<String, Object> rc = dispatch("real-counter-desc", RequestCmd.GET_ALERT_REAL_COUNTER_DESC, NONE, 'P');
        Map<String, Object> ph = dispatch("plugin-helper-desc", RequestCmd.GET_PLUGIN_HELPER_DESC, NONE, 'P');
        canonicalizeDescriptors("real-counter-desc");
        canonicalizeDescriptors("plugin-helper-desc");
        facts.put("real_counter_desc_count", ((List<?>) rc.get("items")).size());
        facts.put("plugin_helper_desc_count", ((List<?>) ph.get("items")).size());

        Path plugin = CWD.resolve("plugin");
        String rule = "// synthetic alert rule\nif ($counter.getFloatValue() > 1.5f) { $counter.warn(\"synthetic\", \"합성 \" + $counter.getObjName()); }\n";
        dispatch("save-alert-script", RequestCmd.SAVE_ALERT_SCRIPTING_CONTETNS, pack(map("counterName", "TPS", "contents", rule)), 'P');
        dispatch("save-alert-config", RequestCmd.SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS, pack(map("counterName", "TPS", "contents", "history_size=150\nsilent_time=300\ncheck_term=20\n")), 'P');
        dispatch("get-alert-script", RequestCmd.GET_ALERT_SCRIPTING_CONTETNS, pack(map("counterName", "TPS")), 'P');
        dispatch("get-alert-config", RequestCmd.GET_ALERT_SCRIPTING_CONFIG_CONTETNS, pack(map("counterName", "TPS")), 'P');
        dispatch("get-alert-script-missing", RequestCmd.GET_ALERT_SCRIPTING_CONTETNS, pack(map("counterName", "NoSuchCounter")), 'P');
        dispatch("save-alert-script-null-contents", RequestCmd.SAVE_ALERT_SCRIPTING_CONTETNS, pack(map("counterName", "ErrorCount")), 'P');
        dispatch("save-alert-script-null-name", RequestCmd.SAVE_ALERT_SCRIPTING_CONTETNS, pack(map("contents", "// orphan\n")), 'P');
        dispatch("save-alert-script-subdir", RequestCmd.SAVE_ALERT_SCRIPTING_CONTETNS, pack(map("counterName", "sub/Elapsed", "contents", "// nested\n")), 'P');
        dispatch("save-alert-script-overwrite", RequestCmd.SAVE_ALERT_SCRIPTING_CONTETNS, pack(map("counterName", "TPS", "contents", "// v2\n")), 'P');
        facts.put("plugin_dir_tree", tree(plugin));
    }

    /**
     * RealCounter/PluginHelper descriptors come from Class.getDeclaredMethods(), whose order differs between
     * JDK 8 runs of the same class file (observed while building this oracle). Each item's MapPack bytes are
     * kept unchanged; only the item order of *.response.bin is rewritten to methodName then parameterTypeNames.
     */
    static void canonicalizeDescriptors(String name) throws Exception {
        Path p = out.resolve(name + ".response.bin");
        DataInputX in = new DataInputX(read(p));
        java.util.TreeMap<String, byte[]> sorted = new java.util.TreeMap<String, byte[]>();
        List<String> observed = new ArrayList<String>();
        while (in.readByte() == TcpFlag.HasNEXT) {
            MapPack mp = (MapPack) in.readPack();
            StringBuilder key = new StringBuilder(mp.getText("methodName")).append('(');
            ListValue params = mp.getList("parameterTypeNames");
            for (int i = 0; i < params.size(); i++) key.append(i == 0 ? "" : ",").append(params.getString(i));
            key.append(')');
            observed.add(key.toString());
            require(sorted.put(key.toString(), pack(mp)) == null, "duplicate descriptor " + key);
        }
        DataOutputX o = new DataOutputX();
        for (byte[] item : sorted.values()) { o.writeByte(TcpFlag.HasNEXT); o.write(item); }
        o.writeByte(TcpFlag.NoNEXT);
        Files.write(p, o.toByteArray());
        for (Object rec : cases) {
            @SuppressWarnings("unchecked") Map<String, Object> r = (Map<String, Object>) rec;
            if (name.equals(r.get("name"))) {
                r.put("items", decodeItems(read(p), 'P'));
                r.put("item_order", "canonicalized: sorted by methodName(parameterTypeNames); original order is Class.getDeclaredMethods() order, which varies between JVM runs");
                r.put("canonical_keys", new ArrayList<Object>(sorted.keySet()));
            }
        }
    }

    /**
     * Direct call of the original RealCounter.getRealCounterDescription / PluginHelper.getPluginHelperDesc
     * (the lists the two descriptor handlers stream). Run once per JDK by scripts/java-management.py.
     * Element sets are written sorted by methodName(parameterTypeNames); raw order is JVM/run dependent.
     */
    static void descriptors() throws Exception {
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        root.put("java_specification_version", System.getProperty("java.specification.version"));
        root.put("RealCounter", describeDescs(scouter.server.plugin.alert.RealCounter.getRealCounterDescription(), true));
        root.put("PluginHelper", describeDescs(scouter.server.plugin.PluginHelper.getPluginHelperDesc(), false));
        writeText("descriptors.java" + System.getProperty("java.specification.version").replace("1.8", "8") + ".json", Json.write(root) + "\n");
        System.out.println("descriptors: java " + System.getProperty("java.specification.version"));
        System.exit(0);
    }

    static Map<String, Object> describeDescs(List<?> descs, boolean realCounter) {
        java.util.TreeMap<String, Object> sorted = new java.util.TreeMap<String, Object>();
        for (Object o : descs) {
            String desc, methodName, ret; List<String> params;
            if (realCounter) { scouter.server.plugin.alert.RealCounter.Desc d = (scouter.server.plugin.alert.RealCounter.Desc) o; desc = d.desc; methodName = d.methodName; ret = d.returnTypeName; params = d.parameterTypeNames; }
            else { scouter.server.plugin.PluginHelper.Desc d = (scouter.server.plugin.PluginHelper.Desc) o; desc = d.desc; methodName = d.methodName; ret = d.returnTypeName; params = d.parameterTypeNames; }
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("methodName", methodName); m.put("returnTypeName", ret); m.put("parameterTypeNames", new ArrayList<Object>(params)); m.put("desc", desc);
            String key = methodName + "(" + String.join(",", params) + ")";
            require(sorted.put(key, m) == null, "duplicate descriptor " + key);
        }
        Map<String, Object> r = new LinkedHashMap<String, Object>();
        r.put("count", descs.size());
        r.put("order", "sorted by methodName(parameterTypeNames)");
        r.put("items", new ArrayList<Object>(sorted.values()));
        return r;
    }

    // ---------------------------------------------------------------- manage (DB delete, remote control)
    static void manage() throws Exception {
        writeConf("scouter.conf", INITIAL_CONF);
        String[] files = {"database/20260101/xlog/xlog.data", "database/20260101/text/text.data", "database/20260102/xlog/xlog.data",
                "database/20260103/counter/real.data", "database/00000000/text/text_service.data", "victim/sentinel.txt"};
        for (String f : files) { Path p = CWD.resolve(f); Files.createDirectories(p.getParent()); Files.write(p, ("synthetic " + f).getBytes(StandardCharsets.UTF_8)); }
        facts.put("tree_initial", tree(CWD.resolve("database")));
        dispatch("db-delete-day", RequestCmd.SERVER_DB_DELETE, pack(map("file", list("/20260101"))), 'P');
        dispatch("db-delete-missing-and-file", RequestCmd.SERVER_DB_DELETE, pack(map("file", list("/no-such-day", "/20260102/xlog/xlog.data"))), 'P');
        dispatch("db-delete-no-list", RequestCmd.SERVER_DB_DELETE, pack(map("other", "x")), 'P');
        dispatch("db-delete-empty-list", RequestCmd.SERVER_DB_DELETE, pack(map("file", list())), 'P');
        dispatch("db-delete-non-text-item", RequestCmd.SERVER_DB_DELETE, pack(map("file", list(20260103L))), 'P');
        facts.put("tree_after_deletes", tree(CWD.resolve("database")));
        dispatch("db-delete-traversal", RequestCmd.SERVER_DB_DELETE, pack(map("file", list("/../victim"))), 'P');
        facts.put("victim_after_traversal", tree(CWD.resolve("victim")));
        facts.put("victim_dir_exists_after_traversal", Files.exists(CWD.resolve("victim")));

        LoginManager.addUser(101L, "synthetic-a", "10.0.0.1", "admin");
        LoginManager.addUser(102L, "synthetic-b", "10.0.0.2", "guest");
        MapPack popup = map("toSession", 102L, "fromSession", 101L, "command", "POPUP_MESSAGE", "message", "synthetic hello", "keeptime", 3000L);
        dispatch("remote-control", RequestCmd.REMOTE_CONTROL, pack(popup), 'P');
        dispatch("check-job-102", RequestCmd.CHECK_JOB, pack(map("session", 102L)), 'P');
        dispatch("check-job-102-empty", RequestCmd.CHECK_JOB, pack(map("session", 102L)), 'P');
        for (int i = 1; i <= 7; i++)
            dispatch("remote-control-burst-" + i, RequestCmd.REMOTE_CONTROL, pack(map("toSession", 102L, "fromSession", 101L, "command", "POPUP_MESSAGE", "message", "burst " + i)), 'P');
        for (int i = 1; i <= 7; i++) dispatch("check-job-burst-" + i, RequestCmd.CHECK_JOB, pack(map("session", 102L)), 'P');
        dispatch("remote-control-invalid-to", RequestCmd.REMOTE_CONTROL, pack(map("toSession", 999L, "fromSession", 101L, "command", "EXIT_CLIENT")), 'P');
        dispatch("check-job-invalid-to", RequestCmd.CHECK_JOB, pack(map("session", 999L)), 'P');
        dispatch("remote-control-invalid-from", RequestCmd.REMOTE_CONTROL, pack(map("toSession", 102L, "fromSession", 888L, "command", "RESTART_CLIENT")), 'P');
        dispatch("check-job-after-invalid-from", RequestCmd.CHECK_JOB, pack(map("session", 102L)), 'P');
        dispatch("remote-control-all", RequestCmd.REMOTE_CONTROL_ALL, pack(map("fromSession", 101L, "command", "REFETCH_COUNTER_XML")), 'P');
        dispatch("check-job-all-101", RequestCmd.CHECK_JOB, pack(map("session", 101L)), 'P');
        dispatch("check-job-all-102", RequestCmd.CHECK_JOB, pack(map("session", 102L)), 'P');
        dispatch("remote-control-all-invalid-from", RequestCmd.REMOTE_CONTROL_ALL, pack(map("fromSession", 777L, "command", "EXIT_CLIENT")), 'P');
        dispatch("check-job-all-invalid-101", RequestCmd.CHECK_JOB, pack(map("session", 101L)), 'P');
        dispatch("check-job-unknown", RequestCmd.CHECK_JOB, pack(map("session", 123456L)), 'P');
        dispatch("check-job-no-session", RequestCmd.CHECK_JOB, pack(map()), 'P');
    }

    // ---------------------------------------------------------------- agent info (OBJECT_REMOVE*)
    static ObjectPack obj(int hash, String type, String name, boolean alive) {
        ObjectPack p = new ObjectPack();
        p.objHash = hash; p.objType = type; p.objName = name; p.address = "10.0.0." + (hash % 250); p.version = "synthetic";
        p.alive = alive; p.wakeup = 1767193200000L; p.tags.put("region", "synthetic");
        return p;
    }

    static void seedObjects() {
        AgentManager.reset(); ObjectRD.reset(); ObjectWR.removed.clear();
        ObjectPack a = obj(1001, "tomcat", "/synthetic/a", true), b = obj(1002, "tomcat", "/synthetic/b", false), c = obj(1003, "host", "/synthetic/c", true);
        ObjectRD.seed(a); ObjectRD.seed(b); ObjectRD.seed(c);
        AgentManager.seed(obj(1001, "tomcat", "/synthetic/a", true));
        AgentManager.seed(obj(1002, "tomcat", "/synthetic/b", false));
    }

    static void agentinfo() throws Exception {
        writeConf("scouter.conf", INITIAL_CONF);
        // Only the response streams are the golden. ObjectWR is a seam that records remove() calls; the original
        // writer thread NPEs on the remove Order (Order.add defaults to true), so no disk result is expected here.
        facts.put("disk_effect", "not executed: ObjectWR seam records ObjectWR.remove(objHash) calls only; original writer NPE means no delete marker (management-commands.md 6.3, 9-6)");
        seedObjects();
        dispatch("today-full-list-initial", RequestCmd.OBJECT_TODAY_FULL_LIST, NONE, 'P');
        dispatch("remove-inactive", RequestCmd.OBJECT_REMOVE_INACTIVE, NONE, 'P');
        facts.put("memory_after_remove_inactive", new ArrayList<Object>(AgentManager.snapshotHashes()));
        dispatch("today-full-list-after-remove-inactive", RequestCmd.OBJECT_TODAY_FULL_LIST, NONE, 'P');
        seedObjects();
        dispatch("remove-in-memory", RequestCmd.OBJECT_REMOVE_IN_MEMORY, value(list(1001L)), 'P');
        facts.put("memory_after_remove_in_memory", new ArrayList<Object>(AgentManager.snapshotHashes()));
        facts.put("objectwr_seam_remove_calls_after_in_memory", new ArrayList<Object>(ObjectWR.removed));
        seedObjects();
        ListValue mixed = new ListValue(); mixed.add(1001L); mixed.add(new TextValue("1002")); mixed.add(new FloatValue(1003.7f)); mixed.add(4294968297L);
        dispatch("remove-permanent-mixed", RequestCmd.OBJECT_REMOVE, value(mixed), 'P');
        facts.put("memory_after_remove", new ArrayList<Object>(AgentManager.snapshotHashes()));
        facts.put("objectwr_seam_remove_calls_after_remove", new ArrayList<Object>(ObjectWR.removed));
        seedObjects();
        dispatch("remove-empty-list", RequestCmd.OBJECT_REMOVE, value(list()), 'P');
        seedObjects();
        dispatch("remove-mappack-body", RequestCmd.OBJECT_REMOVE, pack(map("objHash", 1001L)), 'P');
        facts.put("memory_after_mappack_body", new ArrayList<Object>(AgentManager.snapshotHashes()));
        seedObjects();
        dispatch("remove-text-body", RequestCmd.OBJECT_REMOVE, value(new TextValue("1001")), 'P');
        facts.put("objectwr_seam_remove_calls_after_text_body", new ArrayList<Object>(ObjectWR.removed));
    }

    // ---------------------------------------------------------------- thread list (JVM dependent: shape only)
    static void threadlist() throws Exception {
        writeConf("scouter.conf", INITIAL_CONF);
        facts.put("thread_list_shape", shape(RequestCmd.SERVER_THREAD_LIST, NONE));
        long id = Thread.currentThread().getId();
        facts.put("thread_detail_shape", shape(RequestCmd.SERVER_THREAD_DETAIL, pack(map("id", id))));
        dispatch("thread-detail-zero", RequestCmd.SERVER_THREAD_DETAIL, pack(map("id", 0L)), 'P');
        dispatch("thread-detail-missing-id", RequestCmd.SERVER_THREAD_DETAIL, pack(map()), 'P');
        dispatch("thread-detail-unknown", RequestCmd.SERVER_THREAD_DETAIL, pack(map("id", 9223372036854775807L)), 'P');
        dispatch("thread-detail-text-id", RequestCmd.SERVER_THREAD_DETAIL, pack(map("id", String.valueOf(id))), 'P');
    }

    /** Runs a JVM-state-dependent command and keeps only item count, key order and value types (no bytes). */
    static Map<String, Object> shape(String cmd, byte[] body) throws Exception {
        load();
        DataOutputX dout = new DataOutputX();
        scouter.server.netio.service.ServiceHandlingProxy.process(cmd, new DataInputX(body), dout, true);
        dout.writeByte(TcpFlag.NoNEXT);
        DataInputX in = new DataInputX(dout.toByteArray());
        List<Object> items = new ArrayList<Object>();
        while (in.readByte() == TcpFlag.HasNEXT) {
            Pack p = in.readPack();
            Map<String, Object> keys = new LinkedHashMap<String, Object>();
            if (p instanceof MapPack) {
                Iterator<String> it = ((MapPack) p).keys();
                while (it.hasNext()) {
                    String k = it.next(); Value v = ((MapPack) p).get(k);
                    String type = typeName(v);
                    if (v instanceof ListValue && ((ListValue) v).size() > 0) {
                        java.util.Set<String> elem = new java.util.TreeSet<String>();
                        for (int i = 0; i < ((ListValue) v).size(); i++) elem.add(typeName(((ListValue) v).get(i)));
                        type += "<" + String.join("|", elem) + ">";
                    }
                    keys.put(k, type);
                }
            }
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("pack", (int) p.getPackType()); item.put("keys", keys);
            items.add(item);
        }
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("command", cmd); m.put("request_hex", hex(body)); m.put("items", items);
        return m;
    }

    static String typeName(Value v) {
        switch (v.getValueType()) {
            case ValueEnum.NULL: return "null";
            case ValueEnum.BOOLEAN: return "boolean";
            case ValueEnum.DECIMAL: return "decimal";
            case ValueEnum.FLOAT: return "float";
            case ValueEnum.DOUBLE: return "double";
            case ValueEnum.TEXT: return "text";
            case ValueEnum.LIST: return "list";
            case ValueEnum.MAP: return "map";
            case ValueEnum.BLOB: return "blob";
            default: return "type" + v.getValueType();
        }
    }

    // ---------------------------------------------------------------- verify Go-written XML with original parsers
    static void verifyXml(Path dir) throws Exception {
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        File acc = dir.resolve("account.xml").toFile(), grp = dir.resolve("account_group.xml").toFile(), site = dir.resolve("counters.site.xml").toFile();
        int failures = 0;
        if (acc.exists()) {
            try { root.put("account.xml", describeAccounts(AccountFileHandler.parse(acc))); }
            catch (Throwable t) { root.put("account.xml", "PARSE FAILED: " + t); failures++; }
        }
        if (grp.exists()) {
            try { root.put("account_group.xml", describeGroups(GroupFileHandler.parse(grp))); }
            catch (Throwable t) { root.put("account_group.xml", "PARSE FAILED: " + t); failures++; }
        }
        if (site.exists()) {
            CounterEngine engine = new CounterEngine();
            boolean ok = engine.parse(read(site.toPath()));
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("parse", ok);
            List<Object> types = new ArrayList<Object>();
            for (String name : engine.getAllObjectType()) {
                scouter.lang.ObjectType t = engine.getObjectType(name);
                Map<String, Object> x = new LinkedHashMap<String, Object>();
                x.put("name", name); x.put("disp", t.getDisplayName()); x.put("family", t.getFamily() == null ? null : t.getFamily().getName());
                x.put("icon", t.getIcon()); x.put("subObject", t.isSubObject());
                types.add(x);
            }
            m.put("objectTypes", types);
            root.put("counters.site.xml", m);
            if (!ok) failures++;
        }
        if (root.isEmpty()) { System.out.println("no account.xml/account_group.xml/counters.site.xml in " + dir); System.exit(2); }
        System.out.println(Json.write(root));
        System.exit(failures == 0 ? 0 : 1);
    }
}

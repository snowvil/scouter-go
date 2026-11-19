package telegraf

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const pinnedScouter = "dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5"

// jaxbDump prints the TgConfig JAXB view; the Go dump below mirrors it.
const jaxbDump = `import java.io.*; import javax.xml.bind.*; import scouter.server.support.telegraf.*;
public class Dump {
  static String s(Object o){ return o==null?"<null>":"["+o+"]"; }
  public static void main(String[] a) throws Exception {
    Unmarshaller u = JAXBContext.newInstance(TgConfig.class).createUnmarshaller();
    for (String f : a) {
      StringBuilder b = new StringBuilder();
      try {
        TgConfig c = (TgConfig) u.unmarshal(new File(f));
        b.append("enabled=").append(c.enabled).append(" debug=").append(c.debugEnabled).append(" norm=").append(c.deltaCounterNormalizeDefault).append(" normSec=").append(c.deltaCounterNormalizeDefaultSeconds).append(" dead=").append(c.objectDeadtimeMs).append('\n');
        for (TgmConfig m : c.measurements) {
          b.append(" m name=").append(s(m.measurementName)).append(" en=").append(m.enabled).append(" dbg=").append(m.debugEnabled)
           .append(" fam=").append(s(m.objFamilyBase)).append(m.objFamilyAppendTags).append(" type=").append(s(m.objTypeBase)).append(m.objTypePrependTags).append(m.objTypeAppendTags)
           .append(" icon=").append(s(m.objTypeIcon)).append(" name=").append(s(m.objNameBase)).append(m.objNameAppendTags).append(" host=").append(s(m.hostTag)).append('\n');
          for (TgmConfig.HostMapping h : m.hostMappings) b.append("  h ").append(s(h.telegraf)).append(s(h.scouter)).append('\n');
          for (TgmConfig.TagFilter t : m.tagFilters) b.append("  f ").append(s(t.tag)).append(t.match).append('\n');
          for (TgCounterMapping cm : m.counterMappings) b.append("  c ").append(s(cm.tgFieldName)).append(s(cm.counterName)).append(s(cm.displayName)).append(s(cm.unit)).append(" tot=").append(cm.totalizable).append(" ns=").append(cm.normalizeSeconds).append(" dt=").append(cm.deltaType).append('\n');
        }
      } catch (Exception e) { b.append("ERROR ").append(e.getClass().getSimpleName()).append('\n'); }
      System.out.print("== " + new File(f).getName() + "\n" + b);
    }
  }
}
`

func dumpPtr(s *string) string {
	if s == nil {
		return "<null>"
	}
	return "[" + *s + "]"
}

func dumpList(l []string) string {
	if l == nil {
		return "null"
	}
	return "[" + strings.Join(l, ", ") + "]"
}

func goDump(name string, data []byte) string {
	var b strings.Builder
	fmt.Fprintf(&b, "== %s\n", name)
	f, _, err := ParseFile(data)
	if err != nil {
		b.WriteString("ERROR UnmarshalException\n")
		return b.String()
	}
	str := func(s string) string { return "[" + s + "]" }
	fmt.Fprintf(&b, "enabled=%v debug=%v norm=%v normSec=%d dead=%d\n", f.Enabled, f.DebugEnabled, f.DeltaCounterNormalizeDefault, f.DeltaCounterNormalizeDefaultSeconds, f.ObjectDeadtimeMs)
	for _, m := range f.Measurements {
		fmt.Fprintf(&b, " m name=%s en=%v dbg=%v fam=%s%s type=%s%s%s icon=%s name=%s%s host=%s\n", dumpPtr(m.MeasurementName), m.Enabled, m.DebugEnabled,
			str(m.ObjFamilyBase), dumpList(m.ObjFamilyAppendTags), str(m.ObjTypeBase), dumpList(m.ObjTypePrependTags), dumpList(m.ObjTypeAppendTags),
			str(m.ObjTypeIcon), str(m.ObjNameBase), dumpList(m.ObjNameAppendTags), str(m.HostTag))
		for _, h := range m.HostMappings {
			fmt.Fprintf(&b, "  h %s%s\n", dumpPtr(h.Telegraf), dumpPtr(h.Scouter))
		}
		for _, t := range m.TagFilters {
			fmt.Fprintf(&b, "  f %s%s\n", dumpPtr(t.Tag), dumpList(t.Match))
		}
		for _, c := range m.CounterMappings {
			dt := string(c.DeltaType)
			if dt == "" {
				dt = "null"
			}
			fmt.Fprintf(&b, "  c %s%s%s%s tot=%v ns=%d dt=%s\n", dumpPtr(c.TgFieldName), dumpPtr(c.CounterName), dumpPtr(c.DisplayName), str(c.Unit), c.Totalizable, c.NormalizeSeconds, dt)
		}
	}
	return b.String()
}

// TestJAXBOracle unmarshals the same XML files with the pinned TgConfig on a
// JDK 8 (JAXB RI bundled) and compares the field view with ParseFile. It needs
// JAVA8_HOME and SCOUTER_SOURCE; without them it is skipped (not a pass).
func TestJAXBOracle(t *testing.T) {
	javaHome, source := os.Getenv("JAVA8_HOME"), os.Getenv("SCOUTER_SOURCE")
	if javaHome == "" || source == "" {
		t.Skip("JAVA8_HOME and SCOUTER_SOURCE are required for the JAXB oracle")
	}
	head, err := exec.Command("git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(head)) != pinnedScouter {
		t.Fatalf("pinned source: %s %v", head, err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dump.java"), []byte(jaxbDump), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-d", dir, filepath.Join(dir, "Dump.java"), filepath.Join(source, "scouter.common/src/main/java/scouter/lang/DeltaType.java")}
	for _, n := range []string{"TgConfig", "TgmConfig", "TgCounterMapping"} {
		args = append(args, filepath.Join(source, "scouter.server/src/main/java/scouter/server/support/telegraf", n+".java"))
	}
	if out, err := exec.Command(filepath.Join(javaHome, "bin", "javac"), args...).CombinedOutput(); err != nil {
		t.Fatalf("javac: %s %v", out, err)
	}
	files, _ := filepath.Glob(filepath.Join("testdata", "jaxb", "*.xml"))
	files = append(files, filepath.Join("..", "server", "telegraf-sample.xml"))
	wrong := filepath.Join(dir, "wrong-root.xml")
	os.WriteFile(wrong, []byte("<Other/>"), 0o600)
	files = append(files, wrong)
	out, err := exec.Command(filepath.Join(javaHome, "bin", "java"), append([]string{"-cp", dir, "Dump"}, files...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("java: %s %v", out, err)
	}
	var want strings.Builder
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		want.WriteString(goDump(filepath.Base(f), data))
	}
	if string(out) != want.String() {
		t.Fatalf("JAXB view differs\njava:\n%s\ngo:\n%s", out, want.String())
	}
}

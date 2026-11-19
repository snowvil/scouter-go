package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// zipProbe reads a DB through the original Java classes: CompressUtil.unZip of
// whole xlog.N files, and XLog/Profile records through the original index and
// GZipStore reader. The process default modes are the opposite of the stored
// .conf so the per-file mode must win.
const zipProbe = `import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.lang.reflect.*;
import scouter.io.*;
import scouter.lang.pack.*;
import scouter.server.Configure;
import scouter.server.db.xlog.*;
import scouter.server.db.io.zip.*;
import scouter.util.*;

public class ZipProbe {
    static String hex(byte[] b){StringBuilder s=new StringBuilder();for(byte x:b)s.append(String.format("%02x",x));return s.toString();}
    public static void main(String[] a){
        try{
            if(a[0].equals("unzip")){for(int i=1;i<a.length;i++){byte[] b=CompressUtil.unZip(Files.readAllBytes(Paths.get(a[i])));System.out.println("unzip "+Paths.get(a[i]).getFileName()+" "+b.length+" "+hex(MessageDigest.getInstance("SHA-256").digest(b)));}return;}
            Path root=Paths.get(a[1]).toAbsolutePath();String date=a[2];
            System.setProperty("oracle.root",root.toString());Configure.getInstance().compress_xlog_enabled=false;Configure.getInstance().compress_profile_enabled=false;
            String xfile=root.resolve(date).resolve("xlog").resolve("xlog").toString();
            XLogDataReader xr=new XLogDataReader(date,xfile);XLogIndex xi=new XLogIndex(xfile);XLogProfileDataReader pr=new XLogProfileDataReader(date,xfile);XLogProfileIndex pi=new XLogProfileIndex(xfile);
            for(int i=3;i<a.length;i++){long tx=Long.parseLong(a[i]);XLogPack p=(XLogPack)new DataInputX(xr.read(xi.getByTxid(tx))).readPack();System.out.println("xlog "+p.txid+" "+p.endTime+" "+p.gxid);
                List<?> pos=pi.getByTxid(tx);StringBuilder sb=new StringBuilder();for(Object o:pos)sb.append(hex(pr.read(((Number)o).longValue()))).append(',');System.out.println("profile "+tx+" "+sb);}
            xr.close();xi.close();pr.close();pi.close();GZipStore.getInstance().shutdown();
            Field field=IOChannel.class.getDeclaredField("exec");field.setAccessible(true);ExecutorService pool=(ExecutorService)field.get(null);pool.shutdown();pool.awaitTermination(30,TimeUnit.SECONDS);
        }catch(Throwable e){e.printStackTrace();System.exit(1);}
    }
}
`

// TestJavaReadsGoGzipMembers checks the multi-member GZipStore both ways with
// the original Java classes: a Java-written compressed DB continued by the Go
// writer still passes the Java verify, Java reads the Go-appended records
// through its own index and GZipStore, and Java CompressUtil.unZip returns the
// same logical bytes as the Go decoder for every block (Go-only DB included).
func TestJavaReadsGoGzipMembers(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	source := os.Getenv("SCOUTER_SOURCE")
	if source == "" {
		source = filepath.Join(repo, "..", "scouter")
	}
	if _, err = os.Stat(filepath.Join(source, "scouter.server")); err != nil {
		t.Skip("pinned Java source unavailable; DB oracle not run")
	}
	work := t.TempDir()
	output, err := exec.Command("python3", filepath.Join(repo, "scripts", "java-storage.py"), "--classes-dir", filepath.Join(work, "compiled"), "--source", source).CombinedOutput()
	if err != nil {
		t.Fatalf("actual Java/Scala storage compilation: %v\n%s", err, output)
	}
	probeDir := filepath.Join(work, "probe")
	if err = os.MkdirAll(probeDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(probeDir, "ZipProbe.java"), []byte(zipProbe), 0600); err != nil {
		t.Fatal(err)
	}
	classpath := strings.TrimSpace(string(output))
	if out, err := exec.Command("javac", "--release", "8", "-encoding", "UTF-8", "-cp", classpath, "-d", probeDir, filepath.Join(probeDir, "ZipProbe.java")).CombinedOutput(); err != nil {
		t.Fatalf("javac ZipProbe: %v\n%s", err, out)
	}
	classpath += string(os.PathListSeparator) + probeDir
	java := func(class string, args ...string) string {
		t.Helper()
		out, err := exec.Command("java", append([]string{"-Duser.timezone=Asia/Seoul", "-cp", classpath, class}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("java %s %v: %v\n%s", class, args, err, out)
		}
		return string(out)
	}
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		t.Fatal(err)
	}
	javaDB := filepath.Join(work, "java-db")
	java("StorageOracle", "generate", javaDB)

	// Java writer first, Go appends (records and profile chunks of several sizes).
	appended := filepath.Join(work, "appended")
	if out, err := exec.Command("cp", "-R", javaDB, appended).CombinedOutput(); err != nil {
		t.Fatalf("copy: %v %s", err, out)
	}
	root := filepath.Join(appended, "gzip")
	s, err := Open(Config{Directory: root, Location: loc})
	if err != nil {
		t.Fatal(err)
	}
	chunks := map[int64][][]byte{}
	for i := int64(0); i < 3; i++ {
		tx := 201 + i
		if err = s.PutXLog(&protocol.XLogPack{TxID: tx, EndTime: oracleTime + 5000 + i, GxID: 77, ObjHash: 42, Service: 7, Elapsed: 10}); err != nil {
			t.Fatal(err)
		}
		for j, n := range []int{1, 300, 70000} {
			b := zipPayload(int(tx)+j, n)
			if err = s.PutProfile(&protocol.ProfilePack{Time: oracleTime, TxID: tx, Profile: b}); err != nil {
				t.Fatal(err)
			}
			chunks[tx] = append(chunks[tx], b)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if b := decodeZipFile(t, filepath.Join(root, "20260101", "xlog", "data", "xlog.0")); b.Members != 1+12 || b.Torn != 0 {
		t.Fatalf("Java member + Go members: %d", b.Members)
	}
	t.Log(strings.TrimSpace(java("StorageOracle", "verify", appended)))
	verifyJavaStore(t, root, loc)
	out := java("ZipProbe", "read", root, "20260101", "201", "202", "203")
	for i := int64(0); i < 3; i++ {
		tx := 201 + i
		if want := fmt.Sprintf("xlog %d %d 77\n", tx, oracleTime+5000+i); !strings.Contains(out, want) {
			t.Fatalf("Java read of Go XLog %d:\n%s", tx, out)
		}
		var hexes []string
		for _, b := range chunks[tx] {
			hexes = append(hexes, hex.EncodeToString(b))
		}
		if want := fmt.Sprintf("profile %d %s,\n", tx, strings.Join(hexes, ",")); !strings.Contains(out, want) {
			t.Fatalf("Java read of Go profile %d differs", tx)
		}
	}

	// Go-only compressed DB: every record is its own member.
	goDB := filepath.Join(work, "go-db", "gzip")
	writeJavaStore(t, goDB, loc, true)
	for _, db := range []string{root, goDB} {
		data := filepath.Join(db, "20260101", "xlog", "data")
		blocks, err := filepath.Glob(filepath.Join(data, "xlog.*"))
		if err != nil || len(blocks) == 0 {
			t.Fatal(blocks, err)
		}
		sort.Strings(blocks)
		out := java("ZipProbe", append([]string{"unzip"}, blocks...)...)
		for _, path := range blocks {
			b := decodeZipFile(t, path)
			sum := sha256.Sum256(b.Data)
			if want := fmt.Sprintf("unzip %s %d %x\n", filepath.Base(path), len(b.Data), sum); !strings.Contains(out, want) {
				t.Fatalf("Java unZip differs from Go decode for %s (%d members):\n%s", path, b.Members, out)
			}
		}
	}
	if b := decodeZipFile(t, filepath.Join(goDB, "20260101", "xlog", "data", "xlog.0")); b.Members != 5 {
		t.Fatalf("Go-only DB members %d", b.Members)
	}
}

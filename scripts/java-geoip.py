#!/usr/bin/env python3
"""합성 legacy MaxMind GeoIP(.dat) DB를 만들고 geoip-api 1.3.1 LookupService로 조회한 golden을 생성/비교한다.

원본 Scouter GISDataUtil은 `new LookupService(file, GEOIP_MEMORY_CACHE).getLocation(InetAddress)`를 쓴다.
실제 GeoLiteCity.dat는 저장소에 넣지 않으므로 이 script가 형식 지식으로 작은 DB를 직접 만들고(정상·경계·손상),
tools/java-geoip/GeoipOracle.java를 원본 배포본과 같은 geoip-api-1.3.1.jar(SHA-256 고정)로 실행한다.
결과(.dat 입력, spec.txt, golden.json, oracle-manifest.json)는 internal/geoip/testdata/oracle에 둔다.

사용법:
  python3 -I scripts/java-geoip.py --generate <빈 디렉터리>
  python3 -I scripts/java-geoip.py --check            # 임시 디렉터리에 재생성 후 testdata와 byte 비교
환경 변수: SCOUTER_GEOIP_JAR(기본 ~/.m2/.../geoip-api-1.3.1.jar), SCOUTER_JAVA8_HOME.
"""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools/java-geoip"
DEST = ROOT / "internal/geoip/testdata/oracle"
JAR_SHA256 = "d4e162e4da56504dce2b5566c304be535298859be7c6235852dbc30ecf56e7fc"
DEFAULT_JAR = Path.home() / ".m2/repository/com/maxmind/geoip/geoip-api/1.3.1/geoip-api-1.3.1.jar"

COUNTRY_BEGIN = 16776960
STATE_BEGIN_REV1 = 16000000
# init()이 structure info 뒤 3 byte segment를 읽는 database type(105 보정 후).
SEGMENT_TYPES = {2, 4, 5, 6, 9, 11, 21, 22, 23, 24, 30, 31, 32, 33}
# geoip-api countryCode 배열 index. golden의 countryCodes로 Go 표와 함께 검증된다.
CC = {"--": 0, "BR": 31, "CH": 43, "DE": 56, "FR": 74, "JP": 111, "KR": 119, "US": 225, "O1": 255}
INFO_CITY = b"SYNTHETIC GEOIP CITY 20261015 Build 1 scouter-go oracle fixture"
INFO_COUNTRY = b"SYNTHETIC GEOIP COUNTRY 20261015 Build 1 scouter-go oracle fixture"


def ip2int(s):
    a = [int(x) for x in s.split(".")]
    assert len(a) == 4 and all(0 <= x <= 255 for x in a), s
    return (a[0] << 24) | (a[1] << 16) | (a[2] << 8) | a[3]


def int2ip(v):
    return ".".join(str((v >> s) & 255) for s in (24, 16, 8, 0))


def le(v, n):
    assert 0 <= v < 1 << (8 * n), (v, n)
    return v.to_bytes(n, "little")


def angle(v):
    """(deg + 180) * 10000을 3 byte little-endian으로. ("raw", n)은 그대로."""
    if isinstance(v, tuple):
        return le(v[1], 3)
    return le(round((v + 180) * 10000), 3)


def city_record(cc, region=b"", city=b"", postal=b"", lat=0.0, lon=0.0, metro=None):
    b = bytes([cc]) + region + b"\0" + city + b"\0" + postal + b"\0" + angle(lat) + angle(lon)
    if metro is not None:
        b += le(metro, 3)
    return b


class Trie:
    """legacy GeoIP binary trie. leaf는 ("none",) | ("rec", name) | ("raw", value) | ("end", d) | ("selfloop",)."""

    def __init__(self):
        self.nodes = [[("none",), ("none",)]]

    def add(self, cidr, leaf):
        net, plen = cidr.split("/")
        ip, plen = ip2int(net), int(plen)
        assert 1 <= plen <= 32 and ip & ((1 << (32 - plen)) - 1) == 0, cidr
        cur = 0
        for d in range(plen):
            bit = (ip >> (31 - d)) & 1
            if d == plen - 1:
                assert self.nodes[cur][bit][0] != "node", "add general prefixes before specific ones: " + cidr
                if leaf == ("selfloop",):
                    idx = len(self.nodes)
                    self.nodes.append([("node", idx), ("node", idx)])
                    self.nodes[cur][bit] = ("node", idx)
                else:
                    self.nodes[cur][bit] = leaf
                return
            child = self.nodes[cur][bit]
            if child[0] != "node":
                idx = len(self.nodes)
                self.nodes.append([child, child])
                self.nodes[cur][bit] = ("node", idx)
                cur = idx
            else:
                cur = child[1]


def build_db(entries, records, dbtype, rl=3, seg=None, declared_segments=None, info=INFO_CITY, cut_nodes=None, cut_records=None):
    """[trie][0 pad + records][000 + info][FF FF FF type (segment)] 순서의 .dat를 만든다.

    seg=None이면 segment는 trie node 수(City/Org 형식), 숫자면 고정 segment(Country/Region 형식)이다.
    declared_segments는 trailer에 기록할 segment(손상 시험용, 기본은 실제 segment)이다.
    """
    trie = Trie()
    for cidr, leaf in entries:
        trie.add(cidr, leaf)
    nodes = len(trie.nodes)
    real_seg = nodes if seg is None else seg
    rec_start = nodes * 2 * rl
    area = bytearray()
    offsets = {}
    if records:
        area += b"\0"  # record offset 0은 "미등록"(value == segment)과 같으므로 비워 둔다.
        for name, payload in records.items():
            offsets[name] = len(area)
            area += payload
    effective = dbtype - 105 if dbtype >= 106 and dbtype < 128 else dbtype
    tail = b"\xff\xff\xff" + bytes([dbtype])
    if effective in SEGMENT_TYPES:
        tail += le(real_seg if declared_segments is None else declared_segments, 3)
    info_block = b"\0\0\0" + info if info is not None else b""
    node_bytes_len = rec_start if cut_nodes is None else cut_nodes
    area_len = len(area) if cut_records is None else cut_records
    n = node_bytes_len + area_len + len(info_block) + len(tail)
    full_n = rec_start + len(area) + len(info_block) + len(tail)

    def value(leaf):
        kind = leaf[0]
        if kind == "none":
            return real_seg
        if kind == "rec":
            return real_seg + offsets[leaf[1]]
        if kind == "raw":
            return leaf[1]
        if kind == "node":
            return leaf[1]
        if kind == "end":  # record pointer가 (잘리기 전) 파일 끝에서 leaf[1] byte 앞을 가리킨다.
            assert seg is None
            return real_seg + (full_n - rec_start) - leaf[1]
        raise ValueError(leaf)

    out = bytearray()
    for left, right in trie.nodes:
        out += le(value(left), rl) + le(value(right), rl)
    out = out[:node_bytes_len] + area[:area_len] + info_block + tail
    assert len(out) == n
    return bytes(out)


def city_records():
    r = {
        "seoul": city_record(CC["KR"], b"11", b"Seoul", b"", 37.5665, 126.978),
        "mountain_view": city_record(CC["US"], b"CA", b"Mountain View", b"94043", 37.386, -122.0838, metro=807 * 1000 + 650),
        "no_city": city_record(CC["JP"], b"40", b"", b"", 33.6, 130.4),
        "zurich_latin1": city_record(CC["CH"], b"25", "Zürich".encode("latin-1"), b"8001", 47.3667, 8.55),
        "sao_paulo_utf8": city_record(CC["BR"], b"27", "São Paulo".encode("utf-8"), b"", -23.5475, -46.6361),
        "seoul_utf8": city_record(CC["KR"], b"11", "서울".encode("utf-8"), b"", 37.5665, 126.978),
        "country0": city_record(CC["--"], b"", b"Nowhere", b"", 0.0, 0.0),
        "country255": city_record(CC["O1"], b"", b"Anon", b"", 0.0, 0.0),
        # 60 byte window 안에 city NUL이 없다: city는 마지막 byte가 빠지고 postal null, angle 예외.
        "long_city": city_record(CC["FR"], b"", b"L" * 70, b""),
        # 1+1+52+1+3 = 58: latitude는 읽고 longitude에서 2 byte만 남는다.
        "lon_cut": city_record(CC["FR"], b"", b"K" * 51, b""),
        # 1+1+49+1+6 = 58: rev1 US는 metro 3 byte가 window 밖이라 예외, rev0은 정상.
        "metro_cut": city_record(CC["US"], b"", b"M" * 48, b"", 40.0, -75.0),
        # 1+1+48+1+6+3 = 60: metro가 window 끝에 정확히 맞는다.
        "metro_fit": city_record(CC["US"], b"", b"N" * 47, b"", 40.0, -75.0, metro=501 * 1000 + 212),
        "max_angle": city_record(CC["DE"], b"BE", b"Berlin", b"10115", ("raw", 0xFFFFFF), ("raw", 0xFFFFFF)),
        "min_angle": city_record(CC["DE"], b"BE", b"Berlin", b"10115", ("raw", 0), ("raw", 0)),
    }
    for n in range(256):
        r[f"c{n}"] = city_record(n, b"", f"C{n}".encode(), b"")
    return r


def city_entries():
    e = [
        ("0.0.0.0/32", ("rec", "country0")),
        ("1.0.0.0/8", ("rec", "seoul")),
        ("1.2.3.0/24", ("rec", "mountain_view")),
        ("2.0.0.0/8", ("rec", "no_city")),
        ("3.0.0.0/8", ("rec", "zurich_latin1")),
        ("3.1.0.0/16", ("rec", "sao_paulo_utf8")),
        ("3.2.0.0/16", ("rec", "seoul_utf8")),
        ("4.0.0.0/8", ("none",)),
        ("5.0.0.0/8", ("rec", "country0")),
        ("5.255.0.0/16", ("rec", "country255")),
        ("6.0.0.0/8", ("rec", "long_city")),
        ("6.1.0.0/16", ("rec", "lon_cut")),
        ("7.0.0.0/8", ("rec", "metro_cut")),
        ("7.1.0.0/16", ("rec", "metro_fit")),
        ("8.0.0.0/16", ("raw", 10_000_000)),
        ("8.1.0.0/16", ("end", 0)),
        ("8.2.0.0/16", ("end", 5)),
        ("8.3.0.0/16", ("end", 1)),
        ("10.0.0.0/8", ("rec", "seoul")),
        ("12.0.0.0/8", ("selfloop",)),
        ("13.0.0.0/8", ("rec", "max_angle")),
        ("13.1.0.0/16", ("rec", "min_angle")),
        ("127.0.0.0/8", ("rec", "seoul")),
        ("172.16.0.0/12", ("rec", "seoul")),
        ("192.168.0.0/16", ("rec", "seoul")),
        ("255.255.255.254/31", ("rec", "mountain_view")),
        ("255.255.255.255/32", ("rec", "seoul")),
    ]
    e += [(f"9.{n}.0.0/16", ("rec", f"c{n}")) for n in range(256)]
    return e


def boundary_ips(entries, skip_prefix=None):
    ips = []
    for cidr, _ in entries:
        if skip_prefix and cidr.startswith(skip_prefix):
            continue
        net, plen = cidr.split("/")
        start = ip2int(net)
        end = start + (1 << (32 - int(plen))) - 1
        for v in (start - 1, start, end, end + 1):
            if 0 <= v <= 0xFFFFFFFF:
                ips.append(int2ip(v))
    return ips


GLOBAL_IPS = ["0.0.0.0", "0.0.0.1", "11.0.0.1", "100.64.0.1", "127.0.0.1", "128.0.0.0", "172.31.255.255",
              "192.168.0.1", "223.255.255.255", "255.255.255.253", "255.255.255.254", "255.255.255.255"]
SMALL_IPS = ["0.0.0.0", "1.0.0.0", "1.2.3.4", "2.0.0.1", "3.0.0.1", "4.0.0.1", "7.0.0.1", "7.1.0.1", "8.0.0.1",
             "8.1.0.1", "12.0.0.1", "200.1.2.3", "255.255.255.255"]


def unique(seq):
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def databases():
    """(파일 이름, bytes, 조회 IP 목록) 목록. 순서가 golden 순서다."""
    rec, ent = city_records(), city_entries()
    full_ips = unique(GLOBAL_IPS + boundary_ips(ent, skip_prefix="9.") + [f"9.{n}.7.7" for n in range(256)] + ["9.0.0.0", "9.255.255.255"])
    no9_ips = unique(GLOBAL_IPS + boundary_ips(ent, skip_prefix="9.") + ["9.0.0.0", "9.31.0.1", "9.225.0.1", "9.255.255.255"])
    rev1 = build_db(ent, rec, 2)
    t = Trie()
    for cidr, leaf in ent:
        t.add(cidr, leaf)
    node_area = len(t.nodes) * 6
    record_area = 1 + sum(len(v) for v in rec.values())

    country_ent = [("1.0.0.0/8", ("raw", COUNTRY_BEGIN + CC["KR"])), ("2.0.0.0/8", ("raw", COUNTRY_BEGIN)),
                   ("3.0.0.0/8", ("raw", COUNTRY_BEGIN + CC["US"])), ("255.255.255.255/32", ("raw", COUNTRY_BEGIN + CC["O1"]))]
    region_ent = [("1.0.0.0/8", ("raw", STATE_BEGIN_REV1 + 5)), ("2.0.0.0/8", ("raw", STATE_BEGIN_REV1))]
    org_rec = {"org_a": b"Example Org A\0", "org_b": b"Example Org B\0"}
    org_ent = [("1.0.0.0/8", ("rec", "org_a")), ("2.0.0.0/8", ("raw", 0x80000000)), ("3.0.0.0/8", ("raw", 0xFFFFFFFF)),
               ("4.0.0.0/8", ("rec", "org_b"))]
    filler = b"\x11"
    dbs = [
        ("city_rev1.dat", rev1, full_ips),
        ("city_rev0.dat", build_db(ent, rec, 6), no9_ips),
        ("city_rev1_oldtype.dat", build_db(ent, rec, 2 + 105), SMALL_IPS),
        ("city_rev1_seg0.dat", build_db(ent, rec, 2, declared_segments=0), SMALL_IPS),
        # segment 0xFFFFFF는 trailer 끝 3 byte가 FF FF FF가 되어 delimiter로 먼저 잡힌다(type byte 없음 -> EOF).
        ("city_rev1_segffffff.dat", build_db(ent, rec, 2, declared_segments=0xFFFFFF), SMALL_IPS),
        ("city_rev1_segmax.dat", build_db(ent, rec, 2, declared_segments=0xFFFFFE), SMALL_IPS),
        ("city_rev1_segbig.dat", build_db(ent, rec, 2, declared_segments=len(t.nodes) + 100), SMALL_IPS),
        ("city_rev1_cut_records.dat", build_db(ent, rec, 2, cut_records=record_area // 2), no9_ips),
        ("city_rev1_cut_nodes.dat", build_db(ent, rec, 2, cut_nodes=node_area // 2, cut_records=0), SMALL_IPS),
        ("city_rev1_trunc_trailer.dat", rev1[:-2], []),
        ("city_rev1_trunc_half.dat", rev1[: len(rev1) // 2], SMALL_IPS),
        ("country.dat", build_db(country_ent, {}, 1, seg=COUNTRY_BEGIN, info=INFO_COUNTRY), SMALL_IPS),
        ("country_type106.dat", build_db(country_ent, {}, 106, seg=COUNTRY_BEGIN, info=INFO_COUNTRY), SMALL_IPS),
        ("region_rev1.dat", build_db(region_ent, {}, 3, seg=STATE_BEGIN_REV1, info=INFO_COUNTRY), SMALL_IPS),
        ("org.dat", build_db(org_ent, org_rec, 5, rl=4), SMALL_IPS),
        ("unknown_type13.dat", build_db(ent[:4], rec, 13), SMALL_IPS),
        ("negative_type.dat", build_db(ent[:4], rec, 0xF0), SMALL_IPS),
        ("empty.dat", b"", []),
        ("short2.dat", b"\xff\xff", []),
        ("tiny_nodelim.dat", filler * 10, []),
        ("delim_no_type.dat", filler * 30 + b"\xff\xff\xff", []),
        ("delim_short_segment.dat", filler * 30 + b"\xff\xff\xff\x02\x01", []),
        ("delim_at_start.dat", b"\xff\xff\xff\x02\x05\x00\x00", []),
        ("nodelim_60.dat", filler * 60, []),
        ("nodelim_200.dat", filler * 200, SMALL_IPS),
    ]
    for name, data, _ in dbs:
        # .gitattributes가 text=auto이므로 NUL 없는(=text로 판정될 수 있는) 입력에 줄바꿈 byte가 없어야 한다.
        if b"\0" not in data[:8000]:
            assert b"\n" not in data and b"\r" not in data, name
    return dbs


def run(args, cwd=None):
    result = subprocess.run([str(a) for a in args], cwd=cwd, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError("command failed: " + " ".join(map(str, args))[:400] + "\n" + result.stdout[-6000:] + result.stderr[-6000:])
    return result


def java8_home():
    explicit = os.environ.get("SCOUTER_JAVA8_HOME")
    if explicit:
        return Path(explicit)
    if Path("/usr/libexec/java_home").exists():
        result = subprocess.run(["/usr/libexec/java_home", "-v", "1.8"], capture_output=True, text=True)
        if result.returncode == 0:
            return Path(result.stdout.strip())
    raise RuntimeError("JDK 8 is required (set SCOUTER_JAVA8_HOME)")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def geoip_jar():
    jar = Path(os.environ.get("SCOUTER_GEOIP_JAR", DEFAULT_JAR))
    if not jar.is_file():
        raise RuntimeError(f"geoip-api-1.3.1.jar not found: {jar} (set SCOUTER_GEOIP_JAR)")
    actual = sha256(jar.read_bytes())
    if actual != JAR_SHA256:
        raise RuntimeError(f"geoip-api jar SHA-256 mismatch: expected {JAR_SHA256}, got {actual}")
    return jar


def generate(out):
    jar = geoip_jar()
    jdk = java8_home()
    dbs = databases()
    spec = []
    for name, data, ips in dbs:
        (out / name).write_bytes(data)
        spec.append("db " + name)
        spec += ["ip " + ip for ip in ips]
    (out / "spec.txt").write_text("\n".join(spec) + "\n")
    with tempfile.TemporaryDirectory(prefix="scouter-geoip-oracle-") as work:
        classes = Path(work) / "classes"
        classes.mkdir()
        sources = sorted(TOOLS.glob("*.java"))
        run([jdk / "bin/javac", "-source", "8", "-target", "8", "-encoding", "UTF-8", "-nowarn", "-cp", jar, "-d", classes, *sources])
        run([jdk / "bin/java", "-XX:ActiveProcessorCount=2", "-Dfile.encoding=UTF-8", "-Duser.language=en", "-Duser.country=US",
             "-Djava.io.tmpdir=" + work, "-cp", os.pathsep.join([str(classes), str(jar)]), "GeoipOracle",
             (out / "spec.txt").resolve(), (out / "golden.json").resolve()], cwd=work)
        work_paths = [work, str(Path(work).resolve())]
    json.loads((out / "golden.json").read_text())  # 형식 확인
    manifest = {
        "synthetic_only": True,
        "geoip_api": {"artifact": "com.maxmind.geoip:geoip-api:1.3.1", "sha256": JAR_SHA256,
                      "note": "scouter.server pom.xml dependency and the deployed server/lib jar; scouter.deploy/lib/maxmind.geoip.jar (2015, older API) is only used by the boot test build"},
        "constructor": "new LookupService(file, LookupService.GEOIP_MEMORY_CACHE) then getLocation(InetAddress.getByAddress(4 bytes)) as scouter.server.geoip.GISDataUtil",
        "runtime_jdk": run([jdk / "bin/java", "-version"]).stderr.strip().splitlines(),
        "generator": "python3 -I scripts/java-geoip.py --generate <empty dir>",
        "check": "python3 -I scripts/java-geoip.py --check",
        "oracle_source_checksums": [{"path": str(p.relative_to(ROOT)), "sha256": sha256(p.read_bytes())} for p in sorted(TOOLS.glob("*.java"))],
        "files": [{"name": p.name, "bytes": p.stat().st_size, "sha256": sha256(p.read_bytes())}
                  for p in sorted(out.iterdir()) if p.is_file() and p.name != "oracle-manifest.json"],
    }
    (out / "oracle-manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    leak_check(out, work_paths)


def leak_check(out, extra):
    import getpass
    import socket
    needles = {str(Path.home()), "/private/var/folders", "/var/folders", tempfile.gettempdir(), *extra}
    for name in (getpass.getuser(), socket.gethostname(), socket.gethostname().split(".")[0]):
        if len(name) >= 4:
            needles.add(name)
    found = [f"{p.name}: contains {n!r}" for p in sorted(out.iterdir()) if p.is_file() and p.suffix in (".json", ".txt")
             for n in needles if n and n.encode() in p.read_bytes()]
    if found:
        raise RuntimeError("golden leak check failed:\n" + "\n".join(found))


def compare(golden, fresh):
    problems = []
    gfiles = {p.name for p in golden.iterdir() if p.is_file()}
    ffiles = {p.name for p in fresh.iterdir() if p.is_file()}
    for name in sorted(gfiles ^ ffiles):
        problems.append(f"file set differs: {name} ({'golden only' if name in gfiles else 'fresh only'})")
    for name in sorted(gfiles & ffiles):
        a, b = (golden / name).read_bytes(), (fresh / name).read_bytes()
        if a == b:
            continue
        if name.endswith((".json", ".txt")):
            diff = difflib.unified_diff(a.decode("utf-8", "replace").splitlines(), b.decode("utf-8", "replace").splitlines(),
                                        "golden/" + name, "fresh/" + name, lineterm="", n=1)
            problems.append("\n".join(list(diff)[:60]))
        else:
            problems.append(f"bytes differ: {name} ({len(a)} vs {len(b)} bytes)")
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--generate", type=Path, help="빈(또는 없는) 디렉터리에 입력 .dat와 golden 생성")
    group.add_argument("--check", nargs="?", const=DEST, type=Path, help="임시 디렉터리에 재생성 후 golden과 byte 비교")
    args = parser.parse_args()
    if args.generate:
        out = args.generate
        if out.exists() and any(out.iterdir()):
            parser.error("--generate output must be empty")
        out.mkdir(parents=True, exist_ok=True)
        generate(out.resolve())
        print(f"OK: generated {sum(1 for p in out.iterdir() if p.is_file())} files in {out}")
        return
    golden = args.check.resolve()
    if not (golden / "oracle-manifest.json").is_file():
        parser.error(f"golden manifest not found: {golden}")
    with tempfile.TemporaryDirectory(prefix="scouter-geoip-check-") as tmp:
        fresh = Path(tmp) / "out"
        fresh.mkdir()
        generate(fresh)
        problems = compare(golden, fresh)
    if problems:
        print("\n".join(problems))
        print(f"FAIL: {len(problems)} difference(s) between golden and regenerated oracle output")
        sys.exit(1)
    count = len(json.loads((golden / "oracle-manifest.json").read_text())["files"])
    print(f"OK: regenerated {count} golden files identical ({golden})")


if __name__ == "__main__":
    main()

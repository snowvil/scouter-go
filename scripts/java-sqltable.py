#!/usr/bin/env python3
"""고정 원본 scouter.util.SQLSimpleParser.getCrudInfo를 JDK 8로 직접 실행해 합성 SQL → CRUD 문자열 golden을 만든다.

- 원본 tree는 읽기만 한다. HEAD가 baseline commit이고 SQLSimpleParser.java가 pinned blob과 같은지 확인한 뒤
  그 파일과 tools/java-sqltable/SqlTableOracle.java만 임시 디렉터리에 컴파일한다(외부 의존성 없음).
- 입력은 tools/java-sqltable/cases.json(손으로 쓴 합성 SQL)과 고정 seed 생성기(xorshift, Python random 미사용)의
  합성 SQL이다. 실제 운영 SQL은 넣지 않는다.
- JVM locale은 en_US로 고정한다(String.toUpperCase가 default locale을 쓴다).
- 결과: internal/sqltable/testdata/java_crud_golden.json. --check는 재생성 결과와 파일 바이트를 비교한다.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools/java-sqltable"
GOLDEN = ROOT / "internal/sqltable/testdata/java_crud_golden.json"
PARSER = "scouter.common/src/main/java/scouter/util/SQLSimpleParser.java"
DEFAULT_JDK8 = Path("/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home")
GENERATED = 1200
STRUCTURED = 800
SEED = 0x5C0A7E5


def baseline_commit():
    return json.loads((ROOT / "docs/compatibility/baseline.json").read_text())["scouter"]["commit"]


def run(cmd, **kw):
    return subprocess.run([str(c) for c in cmd], check=True, capture_output=True, **kw).stdout


def guard(source):
    sha = baseline_commit()
    head = run(["git", "-C", source, "rev-parse", "HEAD"], text=True).strip()
    if head != sha:
        raise SystemExit(f"source HEAD {head} != baseline {sha}")
    record = run(["git", "-C", source, "ls-tree", "HEAD", "--", PARSER], text=True).split()
    blob = record[2]
    data = (source / PARSER).read_bytes()
    actual = hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()
    if actual != blob:
        raise SystemExit(f"{PARSER} differs from the pinned blob")
    return sha, blob


class XorShift:
    def __init__(self, seed):
        self.s = seed & 0xFFFFFFFFFFFFFFFF or 1

    def next(self):
        x = self.s
        x ^= (x << 13) & 0xFFFFFFFFFFFFFFFF
        x ^= x >> 7
        x ^= (x << 17) & 0xFFFFFFFFFFFFFFFF
        self.s = x
        return x

    def below(self, n):
        return self.next() % n

    def pick(self, items):
        return items[self.below(len(items))]


KEYWORDS = ["SELECT", "select", "Select", "FROM", "from", "From", "JOIN", "join", "LEFT", "INNER", "OUTER", "ON",
            "INSERT", "insert", "INTO", "into", "UPDATE", "update", "DELETE", "delete", "MERGE", "merge", "USING",
            "SET", "WHERE", "AND", "OR", "IN", "EXISTS", "VALUES", "AS", "UNION", "ALL", "WITH", "GROUP", "BY",
            "ORDER", "DESC", "WHEN", "MATCHED", "THEN", "NOT", "NULL", "IS", "LIKE", "BETWEEN"]
IDENTS = ["t1", "t2", "t3", "t_a", "t_b", "app.t_order", "s1.t_x", "a", "b", "x", "y", "id", "COUNT(*)", "MAX(a)",
          "\"Q t\"", "`bt`", "[br]", "?", "1", "0", "@{1}", ":p1", "*", "a.b", "테이블", "ſelect", "ınto",
          "té", "\U0001F600"]
PUNCT = ["(", ")", ",", "=", "<", ">", "!", "<>", "!=", "<=", ">=", ";", "+", "-", "/", "*", ".", "'"]
LITERALS = ["'x'", "'a b'", "'FROM t9'", "'('", "''", "'--'"]
COMMENTS = ["-- c FROM t8\n", "--", "#", "# h\n", "/* c */", "/*+ hint */", "/*", "*/", "/*/"]
SEPS = [" ", " ", " ", " ", "  ", "\n", "\r\n", "\r", "\t", ""]


def generate(rng, max_tokens):
    out = []
    for _ in range(1 + rng.below(max_tokens)):
        r = rng.below(100)
        if r < 40:
            out.append(rng.pick(KEYWORDS))
        elif r < 70:
            out.append(rng.pick(IDENTS))
        elif r < 88:
            out.append(rng.pick(PUNCT))
        elif r < 94:
            out.append(rng.pick(LITERALS))
        else:
            out.append(rng.pick(COMMENTS))
        out.append(rng.pick(SEPS))
    if rng.below(4) == 0:
        out.pop()
    return "".join(out)


def kw(rng, word):
    return rng.pick([word.upper(), word.lower(), word.capitalize()])


def table(rng):
    t = rng.pick(["t1", "t2", "t3", "t_order", "app.t_item", "s1.t_x", "\"T Q\"", "`bt`", "테이블", "té"])
    if rng.below(3) == 0:
        t += " " + rng.pick(["a", "b", "x", "AS y"])
    return t


def subquery(rng, depth):
    return ["(", *select(rng, depth + 1), ")"]


def select(rng, depth):
    out = [kw(rng, "select"), rng.pick(["*", "a", "a, b", "COUNT(*)", "MAX(a)"])]
    if depth < 3 and rng.below(5) == 0:
        out += [",", *subquery(rng, depth)]
    out += [kw(rng, "from")]
    for i in range(1 + rng.below(3)):
        if i:
            out.append(",")
        out += subquery(rng, depth) + ["s"] if depth < 3 and rng.below(6) == 0 else [table(rng)]
    for _ in range(rng.below(3)):
        out += [rng.pick(["", kw(rng, "left") + " " + kw(rng, "outer"), kw(rng, "inner")]), kw(rng, "join"), table(rng), kw(rng, "on"), "a.id", "=", "b.id"]
    if rng.below(2):
        out += [kw(rng, "where"), "a", rng.pick(["=", "<>", "!=", "<=", ">=", "<", ">"]), rng.pick(["?", "1", "'x FROM t9'", "@{1}"])]
        if depth < 3 and rng.below(3) == 0:
            out += [kw(rng, "and"), "b", kw(rng, "in"), *subquery(rng, depth)]
    return out


def statement(rng):
    r = rng.below(6)
    if r == 0:
        out = [kw(rng, "insert"), kw(rng, "into"), table(rng)]
        out += ["(", "a", ",", "b", ")"] if rng.below(2) else []
        out += select(rng, 1) if rng.below(2) else [kw(rng, "values"), "(", "?", ",", "?", ")"]
    elif r == 1:
        out = [kw(rng, "update"), table(rng), kw(rng, "set"), "a", "=", "?", kw(rng, "where"), "id", kw(rng, "in"), *subquery(rng, 1)]
    elif r == 2:
        out = [kw(rng, "delete"), kw(rng, "from"), table(rng), kw(rng, "where"), "id", "=", "?"]
    elif r == 3:
        out = [kw(rng, "merge"), kw(rng, "into"), table(rng), kw(rng, "using"), table(rng), kw(rng, "on"), "(", "a", "=", "b", ")",
               kw(rng, "when"), kw(rng, "matched"), kw(rng, "then"), kw(rng, "update"), kw(rng, "set"), "a", "=", "b"]
    else:
        out = select(rng, 0)
    noisy = []
    for tok in out:
        if rng.below(25) == 0:
            noisy.append(rng.pick(COMMENTS))
        noisy.append(tok)
    seps = [" ", " ", " ", " ", " ", " ", "\n", "\r\n", "  ", "\t", ""]
    return "".join(t + rng.pick(seps) for t in noisy).rstrip(" ") if rng.below(2) else "".join(t + rng.pick(seps) for t in noisy)


def long_cases():
    joins = " ".join(f"LEFT OUTER JOIN app.t_join{i} j{i} ON j{i}.id = t0.id" for i in range(200))
    inlist = ", ".join(f"'v{i}'" for i in range(2000))
    nested = "SELECT * FROM t0 WHERE a IN " + "(SELECT a FROM t_nest WHERE a IN " * 60 + "(1)" + ")" * 60
    stmts = "\n".join(f"UPDATE t_u{i} SET a = ? WHERE id IN (SELECT id FROM t_s{i}, t_r{i} WHERE x = ?);" for i in range(300))
    return [
        {"name": "long-joins", "sql": f"SELECT t0.a FROM app.t0 t0 {joins} WHERE t0.k IN ({inlist})"},
        {"name": "long-nested", "sql": nested},
        {"name": "long-statements", "sql": stmts},
        # Unbalanced "( SELECT ... FROM" re-scans the rest per SELECT and adds a node per later FROM:
        # Java needs ~0.25 s for 100 and ~3 s for 200 repetitions (super-quadratic).
        {"name": "pathological-nested-50", "sql": "( SELECT a FROM t " * 50},
        {"name": "pathological-nested-150", "sql": "( SELECT a FROM t " * 150},
        {"name": "many-nodes-20000", "sql": "SELECT " * 20000},
    ]


def all_cases(seed=SEED, generated=GENERATED, structured=STRUCTURED):
    cases = json.loads((TOOLS / "cases.json").read_text(encoding="utf-8"))
    cases += long_cases()
    rng = XorShift(seed)
    for i in range(generated):
        cases.append({"name": f"gen-{i:04d}", "sql": generate(rng, 8 if i < 400 else 40)})
    for i in range(structured):
        cases.append({"name": f"stmt-{i:04d}", "sql": statement(rng)})
    names = [c["name"] for c in cases]
    if len(names) != len(set(names)):
        raise SystemExit("duplicate case names")
    return cases


def java_bin(java_home, name):
    return Path(java_home) / "bin" / name


def oracle(source, java_home, seed=SEED, generated=GENERATED, structured=STRUCTURED):
    sha, blob = guard(source)
    cases = all_cases(seed, generated, structured)
    with tempfile.TemporaryDirectory(prefix="java-sqltable-") as tmp:
        classes = Path(tmp) / "classes"
        classes.mkdir()
        run([java_bin(java_home, "javac"), "-encoding", "UTF-8", "-source", "1.8", "-target", "1.8", "-nowarn",
             "-d", classes, source / PARSER, TOOLS / "SqlTableOracle.java"])
        stdin = "".join(f'{c["name"]}\t{base64.b64encode(c["sql"].encode("utf-8")).decode()}\n' for c in cases)
        out = run([java_bin(java_home, "java"), "-Duser.language=en", "-Duser.country=US", "-Dfile.encoding=UTF-8",
                   "-cp", classes, "SqlTableOracle"], input=stdin.encode("utf-8")).decode("utf-8")
    lines = out.splitlines()
    head = lines[0].split("\t")
    if head[0] != "#java.specification.version" or head[1] != "1.8":
        raise SystemExit(f"JDK 8 required, got {lines[0]!r}")
    results = {}
    for line in lines[1:]:
        name, crud, thrown = line.split("\t")
        results[name] = (base64.b64decode(crud).decode("utf-8"), thrown)
    if len(results) != len(cases):
        raise SystemExit("oracle output count mismatch")
    for c in cases:
        c["crud"], c["thrown"] = results[c["name"]]
    doc = {
        "source_commit": sha,
        "parser_path": PARSER,
        "parser_git_blob": blob,
        "harness_sha256": hashlib.sha256((TOOLS / "SqlTableOracle.java").read_bytes()).hexdigest(),
        "cases_sha256": hashlib.sha256((TOOLS / "cases.json").read_bytes()).hexdigest(),
        "generator": {"seed": seed, "token_soup": generated, "structured": structured},
        "java_specification_version": head[1],
        "user_language": head[3],
        "cases": cases,
    }
    return json.dumps(doc, ensure_ascii=False, indent=1) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, default=Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")))
    parser.add_argument("--java-home", default=os.environ.get("JAVA8_HOME") or (str(DEFAULT_JDK8) if DEFAULT_JDK8.exists() else os.environ.get("JAVA_HOME", "")))
    parser.add_argument("--check", action="store_true", help="regenerate and compare with the committed golden")
    parser.add_argument("--extra-out", type=Path, help="ad-hoc differential run: write a larger golden here instead "
                        "(compare with SQLTABLE_GOLDEN=<path> go test ./internal/sqltable -run TestJavaCrudGolden)")
    parser.add_argument("--seed", type=lambda v: int(v, 0), default=SEED)
    parser.add_argument("--count", type=int, default=GENERATED, help="token-soup and structured cases each (with --extra-out)")
    args = parser.parse_args()
    if not args.java_home:
        raise SystemExit("JDK 8 not found: pass --java-home or set JAVA8_HOME")
    if args.extra_out:
        args.extra_out.write_text(oracle(args.source.resolve(), args.java_home, args.seed, args.count, args.count), encoding="utf-8")
        print(f"wrote {args.extra_out}")
        return 0
    text = oracle(args.source.resolve(), args.java_home)
    if args.check:
        current = GOLDEN.read_text(encoding="utf-8") if GOLDEN.exists() else ""
        if current != text:
            print(f"MISMATCH: {GOLDEN.relative_to(ROOT)} differs from the regenerated oracle output", file=sys.stderr)
            return 1
        print(f"OK: {GOLDEN.relative_to(ROOT)} matches the oracle ({text.count(chr(10))} lines)")
        return 0
    GOLDEN.parent.mkdir(parents=True, exist_ok=True)
    GOLDEN.write_text(text, encoding="utf-8")
    print(f"wrote {GOLDEN.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

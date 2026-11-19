#!/usr/bin/env python3
"""고정된 clean Scouter 소스에서 결정적 서버 계약 inventory를 생성한다."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "docs/compatibility/baseline.json"
OUTPUT = ROOT / "docs/compatibility/source-inventory.json"
REQUEST_CMD = "scouter.common/src/main/java/scouter/net/RequestCmd.java"
PACK_ENUM = "scouter.common/src/main/java/scouter/lang/pack/PackEnum.java"
SOURCE_EXTENSIONS = {".java", ".scala"}


def git(source, *args):
    result = subprocess.run(
        ["git", "-C", str(source), *args], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def verify_source(source, expected_commit):
    if Path(git(source, "rev-parse", "--show-toplevel")).resolve() != source:
        raise ValueError("--source는 Scouter Git root여야 합니다")
    commit = git(source, "rev-parse", "HEAD")
    if commit != expected_commit:
        raise ValueError(f"source SHA 불일치: expected={expected_commit}, actual={commit}")
    dirty = git(source, "status", "--porcelain", "--untracked-files=all")
    if dirty:
        raise ValueError("dirty source 거부: 고정 commit의 clean checkout이 필요합니다")
    return commit


def without_comments(text):
    # 문자열 안의 //와 /*는 유지하며 line evidence를 보존한다.
    pattern = r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|//[^\n]*|/\*.*?\*/'
    return re.sub(
        pattern,
        lambda m: re.sub(r"[^\n]", " ", m.group())
        if m.group().startswith(("//", "/*")) else m.group(),
        text,
        flags=re.DOTALL,
    )


def line_at(text, offset):
    return text.count("\n", 0, offset) + 1


def commands(source):
    constants_text = without_comments((source / REQUEST_CMD).read_text(encoding="utf-8"))
    constants = {}
    declaration = r"(?:static\s+final|final\s+static)\s+String\s+(\w+)\s*=\s*([^;]+);"
    for match in re.finditer(declaration, constants_text):
        name, expression = match.groups()
        if name in constants:
            raise ValueError(f"RequestCmd 상수 중복: {name}")
        constants[name] = (expression.strip(), line_at(constants_text, match.start()))

    entries = []
    for path in sorted((source / "scouter.server/src/main").rglob("*")):
        if path.suffix not in SOURCE_EXTENSIONS:
            continue
        text = without_comments(path.read_text(encoding="utf-8"))
        annotations = list(re.finditer(r"@ServiceHandler\s*\(([^)]*)\)", text))
        for index, annotation in enumerate(annotations):
            expression = annotation.group(1).strip()
            name_match = re.fullmatch(r"RequestCmd\.(\w+)", expression)
            if not name_match or name_match.group(1) not in constants:
                raise ValueError(f"handler 상수 resolve 실패: {path.name}: {expression}")
            name = name_match.group(1)
            literal, constant_line = constants[name]
            if not re.fullmatch(r'"(?:\\.|[^"\\])*"', literal):
                raise ValueError(f"지원하지 않는 RequestCmd 상수 표현식: {name}={literal}")
            # 현재 고정 소스의 명령은 JSON과 공통인 Java string escape만 사용한다.
            value = json.loads(literal)
            end = annotations[index + 1].start() if index + 1 < len(annotations) else len(text)
            body = text[annotation.end():end]
            method_pattern = r"\bdef\s+(\w+)\s*\(" if path.suffix == ".scala" else r"\bpublic\s+(?:static\s+)?void\s+(\w+)\s*\("
            method = re.search(method_pattern, body)
            if not method:
                raise ValueError(f"handler 메서드 resolve 실패: {path.name}: {name}")
            entries.append({
                "command_name": name,
                "command_literal": value,
                "constant_source_path": REQUEST_CMD,
                "constant_line": constant_line,
                "source_path": path.relative_to(source).as_posix(),
                "line": line_at(text, annotation.start()),
                "handler": f"{path.stem}.{method.group(1)}",
                "handler_line": line_at(text, annotation.end() + method.start()),
                "implementation_status": "not_implemented",
            })
    if not entries:
        raise ValueError("ServiceHandler가 없습니다")
    for key in ["command_name", "command_literal"]:
        values = [entry[key] for entry in entries]
        if len(values) != len(set(values)):
            raise ValueError(f"handler 명령 중복: {key}")
    return sorted(entries, key=lambda entry: entry["command_name"])


def pack_types(source):
    text = without_comments((source / PACK_ENUM).read_text(encoding="utf-8"))
    pattern = r"(?:static\s+final|final\s+static)\s+byte\s+(\w+)\s*=\s*([^;]+);"
    entries = []
    for match in re.finditer(pattern, text):
        name, expression = match.groups()
        if not re.fullmatch(r"\d+", expression.strip()):
            raise ValueError(f"PackEnum ID resolve 실패: {name}={expression}")
        entries.append({"name": name, "id": int(expression), "source_path": PACK_ENUM,
                        "line": line_at(text, match.start())})
    if not entries or len({entry["id"] for entry in entries}) != len(entries):
        raise ValueError("PackEnum ID 누락 또는 중복")
    return sorted(entries, key=lambda entry: entry["id"])


def source_files(source):
    files, totals = [], {}
    for scope in ["main", "test"]:
        for path in sorted((source / f"scouter.server/src/{scope}").rglob("*")):
            if path.suffix not in SOURCE_EXTENSIONS:
                continue
            data = path.read_bytes()
            lines = len(data.decode("utf-8").splitlines())
            language = "java" if path.suffix == ".java" else "scala"
            key = f"{scope}_{language}"
            total = totals.setdefault(key, {"files": 0, "lines": 0})
            total["files"] += 1
            total["lines"] += lines
            files.append({"source_path": path.relative_to(source).as_posix(),
                          "scope": scope, "language": language, "lines": lines,
                          "sha256": hashlib.sha256(data).hexdigest()})
    return files, totals


def build_inventory(source):
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))["scouter"]
    commit = verify_source(source, baseline["commit"])
    pom = (source / "pom.xml").read_text(encoding="utf-8")
    version_match = re.search(r"<version>([^<]+)</version>", pom)
    if not version_match or version_match.group(1) != baseline["version"]:
        raise ValueError("source POM version이 baseline과 다릅니다")
    command_entries = commands(source)
    files, totals = source_files(source)
    result = {
        "schema_version": 1,
        "source": {"repository": baseline["repository"], "commit": commit,
                   "version": baseline["version"], "clean_required": True},
        "status_policy": "생성 시 모든 서버 명령은 not_implemented. 이 목록은 Go 구현 또는 통합 호환의 증거가 아니다.",
        "statistics": {"source": totals, "service_handler_commands": len(command_entries),
                       "service_handler_files": len({e["source_path"] for e in command_entries})},
        "commands": command_entries,
        "pack_types": pack_types(source),
        "source_files": files,
    }
    verify_source(source, commit)
    return json.dumps(result, ensure_ascii=False, indent=2) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=os.environ.get("SCOUTER_SOURCE"),
                        help="Scouter Git root (SCOUTER_SOURCE 또는 기본 ../scouter)")
    parser.add_argument("--output", type=Path, default=OUTPUT, help="생성 또는 비교할 JSON")
    parser.add_argument("--check", action="store_true", help="기존 파일과 비교만 수행; 파일 쓰기 없음")
    args = parser.parse_args()
    source = (args.source or ROOT.parent / "scouter").resolve()
    try:
        generated = build_inventory(source)
        if args.check:
            if not args.output.is_file() or args.output.read_bytes() != generated.encode("utf-8"):
                raise ValueError("inventory 불일치 또는 파일 누락: 재생성 후 diff를 검토하세요")
            print("inventory check: PASS")
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(generated, encoding="utf-8")
            print("inventory generated: PASS")
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"inventory: FAIL: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

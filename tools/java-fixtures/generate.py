#!/usr/bin/env python3
"""고정 Scouter source에서 synthetic Java oracle를 임시 경로에 생성한다."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
DEST = ROOT / "internal/protocol/testdata/java"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="committed fixture와 비교하고 수정하지 않는다")
    args = parser.parse_args()
    source = Path(os.environ.get("SCOUTER_SOURCE", ROOT.parent / "scouter")).resolve()
    baseline = json.loads((ROOT / "docs/compatibility/baseline.json").read_text())["scouter"]
    sha = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if sha != baseline["commit"]:
        raise SystemExit(f"source SHA mismatch: expected {baseline['commit']}, got {sha}")
    dirty = subprocess.check_output(["git", "-C", str(source), "status", "--porcelain", "--", "scouter.common"], text=True)
    if dirty:
        raise SystemExit("scouter.common source is modified; oracle generation rejected")
    with tempfile.TemporaryDirectory(prefix="scouter-java-fixtures-") as tmp:
        temp = Path(tmp)
        classes = temp / "classes"
        output = temp / "fixtures"
        classes.mkdir()
        output.mkdir()
        files = sorted((source / "scouter.common/src/main/java").rglob("*.java"))
        oracles = sorted((ROOT / "tools/java-fixtures").glob("*Oracle.java"))
        files.extend(oracles)
        # javac argfile quoting, never shell interpolation; output is outside source.
        argfile = temp / "sources.txt"
        argfile.write_text("\n".join('"' + str(p).replace("\\", "\\\\").replace('"', '\\"') + '"' for p in files))
        result = subprocess.run(["javac", "--release", "11", "-encoding", "UTF-8", "-d", str(classes), "@" + str(argfile)], capture_output=True, text=True)
        if result.returncode:
            raise SystemExit(result.stderr)
        for oracle in oracles:
            subprocess.run(["java", "-cp", str(classes), oracle.stem, str(output)], check=True)
        bins = sorted(output.glob("*.bin"))
        manifest = {"source_commit": sha, "source_version": baseline["version"], "synthetic_only": True,
                    "files": [{"name": p.name, "bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in bins]}
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        if args.check:
            expected = {p.name: p.read_bytes() for p in DEST.glob("*") if p.is_file()}
            generated = {p.name: p.read_bytes() for p in output.iterdir()}
            if expected != generated:
                changed = sorted(k for k in expected.keys() | generated.keys() if expected.get(k) != generated.get(k))
                raise SystemExit("fixture drift: " + ", ".join(changed))
            print(f"PASS: {len(bins)} Java oracle fixtures, source {sha}")
        else:
            DEST.mkdir(parents=True, exist_ok=True)
            old = {p.name for p in DEST.iterdir() if p.is_file()}
            for p in output.iterdir():
                shutil.copyfile(p, DEST / p.name)
                old.discard(p.name)
            for name in old:
                (DEST / name).unlink()
            print(f"Generated {len(bins)} synthetic Java oracle fixtures, source {sha}")


if __name__ == "__main__":
    main()

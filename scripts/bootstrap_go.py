#!/usr/bin/env python3
"""Install the pinned official Go archive locally after SHA-256 verification."""

import hashlib
import os
from pathlib import Path
import platform
import subprocess
import sys
import tarfile
import tempfile


ROOT = Path(__file__).resolve().parents[1]
VERSION = "1.27.2"
# https://go.dev/dl/ — pin both the release and its official archive digest.
SHA256 = {
    ("Darwin", "arm64"): "76812b213b1b2302c978d28fa52fa92d541704b9e7d9d5db8002c50e4018c4c5",
    ("Darwin", "amd64"): "587b59182488b23aa6e5fc25110405a3e0e5b38ed2f5b2f46ed13c32aee356fe",
    ("Linux", "arm64"): "94f3e30b8e374bc285e7dadc11e0865726b9bc6e85b841ccceaabc0214c6b7c8",
    ("Linux", "amd64"): "ecbadb99091a3f46e31f5f934b068b1864eafa7995211b39eaddf76996045fe5",
}


def main():
    if sys.version_info < (3, 12):
        raise SystemExit("Python 3.12+ is required for safe archive extraction")
    if (ROOT / ".go-version").read_text().strip() != VERSION:
        raise SystemExit("Update bootstrap archive checksums with .go-version")
    machine = platform.machine().lower()
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(machine, machine)
    system = platform.system()
    checksum = SHA256.get((system, arch))
    if checksum is None:
        raise SystemExit("Unsupported platform; install the pinned Go release from go.dev/dl")
    tools = ROOT / ".tools"
    target = tools / "go"
    version_env = {**os.environ, "GOTOOLCHAIN": "local"}
    if target.exists():
        result = subprocess.run(
            [str(target / "bin/go"), "version"],
            check=True, text=True, capture_output=True, env=version_env,
        )
        if result.stdout.split()[2] != f"go{VERSION}":
            raise SystemExit("Existing .tools/go has another version; move it before installing")
        print(result.stdout.strip())
        return
    tools.mkdir(exist_ok=True)
    filename = f"go{VERSION}.{system.lower()}-{arch}.tar.gz"
    with tempfile.TemporaryDirectory(prefix="go-install-", dir=tools) as work:
        staging = Path(work)
        archive = staging / filename
        # curl uses the host trust store; TLS verification remains enabled.
        subprocess.run([
            "curl", "--fail", "--silent", "--show-error", "--location",
            "--proto", "=https", "--proto-redir", "=https",
            "--connect-timeout", "30", "--max-time", "300",
            "--output", str(archive), f"https://go.dev/dl/{filename}",
        ], check=True)
        digest = hashlib.sha256()
        with archive.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        if digest.hexdigest() != checksum:
            raise SystemExit("Go archive checksum mismatch; installation aborted")
        with tarfile.open(archive, "r:gz") as bundle:
            bundle.extractall(staging, filter="data")
        subprocess.run([str(staging / "go/bin/go"), "version"], check=True, env=version_env)
        (staging / "go").rename(target)
    print(f"Installed {target}; make uses this toolchain automatically")


if __name__ == "__main__":
    main()

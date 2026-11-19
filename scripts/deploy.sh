#!/bin/sh
# deploy.sh — build the Linux binaries and copy them, with scouter-go.sh, to a
# destination directory. POSIX sh. Run from any OS that has Go (make build-linux).
#
#   scripts/deploy.sh [options] <dest>
#
# <dest> is a local directory (/opt/scouter-go) or a remote one reachable with
# ssh/scp ([user@]host:/opt/scouter-go). It is created if missing.
#
# Options:
#   --arch <amd64|arm64>  target GOARCH (default amd64, or LINUX_ARCH)
#   --no-build            reuse dist/linux-<arch>/ instead of running make build-linux
#   --restart             run "<dest>/scouter-go.sh restart" after copying
#   --dry-run             print what would be copied and exit
#
# Files deployed: scouter-go, scouter-db, scouter-replay, SHA256SUMS, scouter-go.sh.
# Each file is copied to a temporary name and renamed into place, so a running
# server keeps its old inode until the rename and is never left with a partial
# or "Text file busy" binary. database/, logs/, conf/ and scouter-go.env under <dest> are not
# touched. The checksums are verified at the destination after the copy.
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
ARCH=${LINUX_ARCH:-amd64}
BUILD=yes
RESTART=no
DRY_RUN=no
DEST=

usage() {
	echo "usage: $0 [--arch amd64|arm64] [--no-build] [--restart] [--dry-run] <dest-dir | [user@]host:dest-dir>" >&2
	exit 2
}

while [ $# -gt 0 ]; do
	case $1 in
	--arch)
		[ $# -ge 2 ] || usage
		ARCH=$2
		shift 2
		;;
	--no-build) BUILD=no; shift ;;
	--restart) RESTART=yes; shift ;;
	--dry-run) DRY_RUN=yes; shift ;;
	-h | --help) usage ;;
	--) shift; break ;;
	-*) echo "unknown option: $1" >&2; usage ;;
	*)
		[ -z "$DEST" ] || usage
		DEST=$1
		shift
		;;
	esac
done
[ $# -eq 0 ] && [ -n "$DEST" ] || usage

case $ARCH in
amd64 | arm64) ;;
*) echo "unsupported --arch: $ARCH (amd64 or arm64)" >&2; exit 2 ;;
esac

SRC="$ROOT/dist/linux-$ARCH"
FILES="scouter-go scouter-db scouter-replay SHA256SUMS"

# "host:path" is remote; a bare path (including C:-less absolute/relative) is local.
REMOTE=
DEST_DIR=$DEST
case $DEST in
*:*)
	REMOTE=${DEST%%:*}
	DEST_DIR=${DEST#*:}
	[ -n "$REMOTE" ] && [ -n "$DEST_DIR" ] || usage
	;;
esac

# Run a shell command at the destination (locally or over ssh).
at_dest() {
	if [ -n "$REMOTE" ]; then
		ssh "$REMOTE" "$1"
	else
		sh -c "$1"
	fi
}

# Copy a local file to <dest-dir>/<name>.
copy_to_dest() {
	if [ -n "$REMOTE" ]; then
		scp -q "$1" "$REMOTE:$2"
	else
		cp "$1" "$2"
	fi
}

if [ "$BUILD" = yes ]; then
	echo "==> make build-linux LINUX_ARCH=$ARCH"
	if [ "$DRY_RUN" = no ]; then
		(cd "$ROOT" && make build-linux LINUX_ARCH="$ARCH")
	fi
fi

for f in $FILES; do
	[ -f "$SRC/$f" ] || [ "$DRY_RUN" = yes ] || {
		echo "missing $SRC/$f (run without --no-build or make build-linux)" >&2
		exit 1
	}
done

echo "==> deploy dist/linux-$ARCH/ + scripts/scouter-go.sh -> $DEST"
if [ "$DRY_RUN" = yes ]; then
	for f in $FILES; do echo "  $SRC/$f -> $DEST_DIR/$f"; done
	echo "  $ROOT/scripts/scouter-go.sh -> $DEST_DIR/scouter-go.sh"
	[ "$RESTART" = yes ] && echo "  then: $DEST_DIR/scouter-go.sh restart"
	exit 0
fi

# Unique suffix so two deploys to the same directory do not collide.
TMP_SUFFIX=".deploy.$$.$(date +%s)"
at_dest "mkdir -p '$DEST_DIR'"

cleanup() {
	at_dest "rm -f '$DEST_DIR'/*'$TMP_SUFFIX'" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for f in $FILES; do
	copy_to_dest "$SRC/$f" "$DEST_DIR/$f$TMP_SUFFIX"
done
copy_to_dest "$ROOT/scripts/scouter-go.sh" "$DEST_DIR/scouter-go.sh$TMP_SUFFIX"

# Verify the copied binaries against the shipped SHA256SUMS before renaming.
# Linux has sha256sum; macOS (local deploys) has shasum.
at_dest "cd '$DEST_DIR' && \
	if command -v sha256sum >/dev/null 2>&1; then SUM='sha256sum'; else SUM='shasum -a 256'; fi && \
	sed 's/\$/$TMP_SUFFIX/' 'SHA256SUMS$TMP_SUFFIX' | \$SUM -c --quiet -" || {
	echo "checksum verification failed at $DEST; temporary files removed, existing files untouched" >&2
	exit 1
}

# Rename into place: executable bit first, then atomic replace per file.
at_dest "cd '$DEST_DIR' && \
	chmod 0755 'scouter-go$TMP_SUFFIX' 'scouter-db$TMP_SUFFIX' 'scouter-replay$TMP_SUFFIX' 'scouter-go.sh$TMP_SUFFIX' && \
	for f in $FILES scouter-go.sh; do mv -f \"\$f$TMP_SUFFIX\" \"\$f\"; done"
trap - EXIT INT TERM

echo "deployed to $DEST:"
at_dest "cd '$DEST_DIR' && ls -l $FILES scouter-go.sh"

if [ "$RESTART" = yes ]; then
	echo "==> $DEST_DIR/scouter-go.sh restart"
	at_dest "'$DEST_DIR/scouter-go.sh' restart"
fi

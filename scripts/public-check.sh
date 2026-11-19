#!/bin/sh
# public-check.sh — refuse content that must not reach a public repository.
# POSIX sh; needs git. Run it before every commit and push (make public-check).
#
#   scripts/public-check.sh [--staged]
#
# Without options it scans every tracked file of the working tree; --staged
# scans only the staged content (the form a pre-commit hook wants).
#
# Two kinds of rule:
#   1. Generic rules, kept in this file: local absolute paths, private keys
#      and well-known token formats, e-mail addresses outside the allowed
#      domains, tracked executables and archives, tracked host settings.
#   2. A private deny list: one extended regular expression per line in
#      .public-denylist at the repository root (or the file PUBLIC_DENYLIST
#      names). It holds what cannot be written here because writing it would
#      publish it: organisation, service and host names, internal address
#      ranges, account names. The file is ignored by git and is never
#      committed. Lines starting with # and empty lines are skipped. The same
#      expressions are applied to the author and committer of every commit
#      reachable from HEAD.
#
# Exit status: 0 clean, 1 findings, 2 usage or environment error.
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
export LC_ALL=C

MODE=tree
case ${1:-} in
"") ;;
--staged) MODE=staged ;;
*) echo "usage: $0 [--staged]" >&2; exit 2 ;;
esac

DENYLIST=${PUBLIC_DENYLIST:-$ROOT/.public-denylist}
FOUND=0

# scan <label> <extended regex>: print matching lines of the scanned content.
scan() {
	label=$1
	pattern=$2
	if [ "$MODE" = staged ]; then
		out=$(git grep --cached -naIE -e "$pattern" -- . ':!scripts/public-check.sh' || true)
	else
		out=$(git grep -naIE -e "$pattern" -- . ':!scripts/public-check.sh' || true)
	fi
	if [ -n "$out" ]; then
		FOUND=1
		echo "== $label"
		echo "$out" | cut -c1-200
	fi
}

scan "local absolute path" '(/Users/|/home/)[A-Za-z0-9._-]+/|[A-Za-z]:\\Users\\|/private/tmp/'
scan "private key" 'BEGIN [A-Z ]*PRIVATE KEY'
scan "access token format" 'AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{10,}|eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.'

# E-mail addresses: only reserved and upstream-default domains are expected.
if [ "$MODE" = staged ]; then
	mails=$(git grep --cached -naIoE -e '[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}' -- . || true)
else
	mails=$(git grep -naIoE -e '[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}' -- . || true)
fi
mails=$(echo "$mails" | grep -vE '@([A-Za-z0-9-]+\.)*(example\.(com|org|net)|invalid|test|localhost|scouter\.com|users\.noreply\.github\.com|anthropic\.com)$' || true)
if [ -n "$mails" ]; then
	FOUND=1
	echo "== e-mail address outside the allowed domains"
	echo "$mails" | cut -c1-200
fi

# Tracked files that are deployment settings, or binaries that belong in a release.
names=$(git ls-files | grep -E '(^|/)(scouter-go\.env|[^/]*\.env|[^/]*\.pem|[^/]*\.key|[^/]*\.p12|[^/]*\.jks|[^/]*\.hprof|[^/]*\.jar|[^/]*\.war|[^/]*\.zip|[^/]*\.tar|[^/]*\.tgz|[^/]*\.tar\.gz)$|^conf/' || true)
if [ -n "$names" ]; then
	FOUND=1
	echo "== tracked settings, key or archive file"
	echo "$names"
fi

if [ -f "$DENYLIST" ]; then
	while IFS= read -r line || [ -n "$line" ]; do
		case $line in
		"" | "#"*) continue ;;
		esac
		scan "deny list: $line" "$line"
		authors=$(git log --format='%an <%ae> | %cn <%ce>' HEAD 2>/dev/null | sort -u | grep -E -e "$line" || true)
		if [ -n "$authors" ]; then
			FOUND=1
			echo "== deny list in commit author or committer: $line"
			echo "$authors"
		fi
	done <"$DENYLIST"
else
	echo "note: no deny list at $DENYLIST; only the generic rules ran" >&2
fi

if [ "$FOUND" -ne 0 ]; then
	echo "public-check: FAIL" >&2
	exit 1
fi
echo "public-check: PASS"

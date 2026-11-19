#!/bin/sh
# scouter-go.sh — start/stop/restart/status the Scouter Go server as a
# background process on a Linux host. POSIX sh; no bash, systemd or Java needed.
#
#   scouter-go.sh {start|stop|restart|status|args}
#
#   start    run in the background, wait for "Scouter Go listening"
#   stop     SIGTERM, then SIGKILL after SCOUTER_STOP_TIMEOUT seconds (30)
#   status   pid, ps line, /health (needs curl) and the last stats line; exit 3 if stopped
#   args     print the server arguments start would use, one per line
#
# Layout (all relative to this script's directory unless overridden). The
# server runs from this directory, so the <pid>.scouter exit file and relative
# paths in scouter.conf resolve here:
#   ./scouter-go       server binary (make build-linux)
#   ./database         Scouter file database (--db)
#   ./logs             stdout/stderr log (scouter-go.log), pid file, and the Java-style
#                      server-yyyyMMdd.log / request-yyyyMMdd.log (--log-dir)
#   ./conf             scouter.conf (--conf), account.xml/account_group.xml,
#                      scouter-telegraf.xml, plugin/ (.alert/.conf); all optional
#                      (without scouter.conf the server runs on flags and defaults)
#
# Override anything with environment variables, e.g.
#   SCOUTER_UDP=0.0.0.0:6100 SCOUTER_DB=/data/scouter ./scouter-go.sh start
# or put the same SCOUTER_*=value lines in ./scouter-go.env (SCOUTER_ENV_FILE).
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
# Optional per-host settings file (plain SCOUTER_*=value lines, sourced before
# the defaults below) so start/stop/status do not need the variables repeated.
ENV_FILE=${SCOUTER_ENV_FILE:-"$HERE/scouter-go.env"}
# shellcheck disable=SC1090
[ -f "$ENV_FILE" ] && . "$ENV_FILE"
BIN=${SCOUTER_BIN:-"$HERE/scouter-go"}
DB=${SCOUTER_DB:-"$HERE/database"}
LOG_DIR=${SCOUTER_LOG_DIR:-"$HERE/logs"}
LOG_FILE=${SCOUTER_LOG_FILE:-"$LOG_DIR/scouter-go.log"}
PID_FILE=${SCOUTER_PID_FILE:-"$LOG_DIR/scouter-go.pid"}
CONF_DIR=${SCOUTER_CONF_DIR:-"$HERE/conf"}
# Java scouter.conf read by --conf; "" disables the file (flags only). A
# path set here must exist; the default ./conf/scouter.conf may be absent.
CONF_GIVEN=${SCOUTER_CONF+x}
SCOUTER_CONF=${SCOUTER_CONF-"$CONF_DIR/scouter.conf"}

# Listen addresses. Agents and Clients use the same port (Java default 6100).
UDP=${SCOUTER_UDP:-0.0.0.0:6100}
TCP=${SCOUTER_TCP:-0.0.0.0:6100}
# Operational HTTP (GET /health JSON, /debug/pprof). Loopback only; "" disables.
# Unset, it is 127.0.0.1:6180, or 127.0.0.1:6181 when the Java HTTP servlets
# take port 6180 (see http_addr).
HTTP_GIVEN=${SCOUTER_HTTP+x}
HTTP=${SCOUTER_HTTP-127.0.0.1:6180}
# Human-readable throughput/latency line in the log; 0 disables.
STATS_INTERVAL=${SCOUTER_STATS_INTERVAL:-10s}
TIMEZONE=${SCOUTER_TIMEZONE:-Asia/Seoul}
# Java server_id shown as the server node name in the Client (default host name).
SERVER_ID=${SCOUTER_SERVER_ID:-}
# Like the Java server, accounts come from conf/account.xml and
# account_group.xml by default. Setting SCOUTER_USER/SCOUTER_PASSWORD selects
# the single-credential mode instead (the account files are then ignored).
USER_NAME=${SCOUTER_USER:-}
PASSWORD=${SCOUTER_PASSWORD:-}
# Retention like the Java AutoDeleteScheduler ("yes" adds --purge; the
# mgr_purge_* day values come from SCOUTER_EXTRA_ARGS).
PURGE=${SCOUTER_PURGE:-no}
# Pulse Kafka stream: path to a Java-style properties file with
# ext_plugin_pulse_* keys; "" keeps it off.
PULSE_CONF=${SCOUTER_PULSE_CONF:-}
# Go runtime limits for a shared host (empty = unset).
GOMAXPROCS_VALUE=${SCOUTER_GOMAXPROCS:-}
GOMEMLIMIT_VALUE=${SCOUTER_GOMEMLIMIT:-}
# Anything else, e.g. "--compress-xlog --udp-queue 2048 --mgr-purge-keep-days 30".
EXTRA_ARGS=${SCOUTER_EXTRA_ARGS:-}
STOP_TIMEOUT=${SCOUTER_STOP_TIMEOUT:-30}
# Whole seconds only: anything else would make stop wait forever without a
# SIGKILL. Leading zeros are dropped ("08" is not octal in $(( ))).
case $STOP_TIMEOUT in
*[!0-9]*)
	echo "SCOUTER_STOP_TIMEOUT must be whole seconds, got '$STOP_TIMEOUT'" >&2
	exit 2
	;;
esac
STOP_TIMEOUT=$(echo "$STOP_TIMEOUT" | sed 's/^0*\([0-9]\)/\1/')
# The server bounds its own graceful shutdown (--shutdown-timeout, default
# 20s) and then logs the stalled stage and the queued records and exits 3.
# With SCOUTER_STOP_TIMEOUT set, that budget follows it: 10s less, at least
# 5s, so the server gives up and logs before stop sends SIGKILL. A
# --shutdown-timeout in SCOUTER_EXTRA_ARGS wins.
shutdown_timeout() {
	[ -n "${SCOUTER_STOP_TIMEOUT:-}" ] || return 0
	t=$((STOP_TIMEOUT - 10))
	[ "$t" -lt 5 ] && t=5
	echo "${t}s"
}

# conf_value KEY prints the last KEY value in scouter.conf, or nothing. It
# reads plain Java properties lines (key=value, key: value, key value, a bare
# key is empty); escapes, '\' continuations, ${...} and the environment
# fallback are left to the server, which refuses a clashing --http with the
# fix in its log. Relative paths resolve in $HERE like the server's.
conf_value() {
	(
		cd "$HERE" || exit 0
		[ -n "$SCOUTER_CONF" ] && [ -f "$SCOUTER_CONF" ] || exit 0
		awk -v k="$1" '
		{ sub(/\r$/, ""); sub(/^[ \t\f]+/, "") }
		/^[#!]/ { next }
		{
			n = match($0, /[=: \t\f]/)
			if (n == 0) { if ($0 == k) { val = ""; seen = 1 }; next }
			if (substr($0, 1, n - 1) != k) next
			sep = substr($0, n, 1)
			v = substr($0, n + 1)
			sub(/^[ \t\f]+/, "", v)
			if (sep != "=" && sep != ":") { sub(/^[=:]/, "", v); sub(/^[ \t\f]+/, "", v) }
			val = v
		}
		END { print val }' "$SCOUTER_CONF"
	)
}

# telegraf_on reports whether conf/scouter-telegraf.xml exists with a
# top-level <enabled> that JAXB reads as true (the default when absent; an
# empty element is false).
telegraf_on() {
	(
		cd "$HERE" || exit 1
		f="$CONF_DIR/scouter-telegraf.xml"
		[ -f "$f" ] || exit 1
		v=$(awk '/<measurements/ { exit }
			/<enabled\/>/ { f = 1; v = "" }
			match($0, /<enabled>[^<]*</) { f = 1; v = substr($0, RSTART + 9, RLENGTH - 10) }
			END { if (f) print "=" v }' "$f" | sed 's/^=[[:space:]]*/=/')
		case $v in
		'' | =1* | =true*) exit 0 ;;
		esac
		exit 1
	)
}

# http_addr prints the --http address. The Java HTTP servlets bind every
# interface on net_http_port (default 6180) when net_http_server_enabled=true
# or a scouter-telegraf.xml is enabled, and the server refuses two listeners
# on one port. Without an explicit SCOUTER_HTTP, /health then moves from
# 127.0.0.1:6180 to 127.0.0.1:6181.
http_addr() {
	if [ -n "$HTTP_GIVEN" ] || [ -z "$HTTP" ]; then
		echo "$HTTP"
		return 0
	fi
	enabled=$(conf_value net_http_server_enabled | sed 's/[[:space:]]*$//' | tr '[:upper:]' '[:lower:]')
	# Without a conf file (SCOUTER_CONF="") the XML does not turn the servlets on.
	if [ "$enabled" != true ] && { [ -z "$SCOUTER_CONF" ] || ! telegraf_on; }; then
		echo "$HTTP"
		return 0
	fi
	# Decimal like the server: drop a leading + and zeros (no octal in $(( ))).
	port=$(conf_value net_http_port | sed 's/[[:space:]]*$//; s/^+//; s/^0*\([0-9]\)/\1/')
	case $port in
	'' | *[!0-9]*) port=6180 ;;
	esac
	if [ "${HTTP##*:}" = "$port" ] && [ "$port" -lt 65535 ]; then
		echo "127.0.0.1:$((port + 1))"
	else
		echo "$HTTP"
	fi
}

running_pid() {
	[ -f "$PID_FILE" ] || return 1
	pid=$(cat "$PID_FILE" 2>/dev/null) || return 1
	[ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 1
	echo "$pid"
}

build_args() {
	set -- --db "$DB" --udp "$UDP" --tcp "$TCP" --timezone "$TIMEZONE" \
		--stats-interval "$STATS_INTERVAL" \
		--log-dir "$LOG_DIR" --plugin-dir "$CONF_DIR/plugin" \
		--telegraf-config "$CONF_DIR/scouter-telegraf.xml"
	# Unset SCOUTER_CONF with a missing ./conf/scouter.conf omits --conf: the
	# server, started from this directory, then treats its default path as
	# optional (flags and defaults) and still takes the account files and
	# counters.site.xml from ./conf.
	if [ -z "$SCOUTER_CONF" ]; then
		set -- "$@" --conf=
	elif [ -n "$CONF_GIVEN" ] || [ -f "$SCOUTER_CONF" ] || [ "$CONF_DIR" != "$HERE/conf" ]; then
		set -- "$@" --conf "$SCOUTER_CONF"
	fi
	if [ -n "$USER_NAME" ] || [ -n "$PASSWORD" ]; then
		set -- "$@" --user "${USER_NAME:-admin}" --password "${PASSWORD:-admin}"
	fi
	[ -n "$SERVER_ID" ] && set -- "$@" --server-id "$SERVER_ID"
	http=$(http_addr)
	[ -n "$http" ] && set -- "$@" --http "$http"
	[ "$PURGE" = yes ] && set -- "$@" --purge
	[ -n "$PULSE_CONF" ] && set -- "$@" --pulse-conf "$PULSE_CONF"
	case " $EXTRA_ARGS " in
	*" -shutdown-timeout"* | *" --shutdown-timeout"*) ;;
	*)
		budget=$(shutdown_timeout)
		[ -n "$budget" ] && set -- "$@" --shutdown-timeout "$budget"
		;;
	esac
	# EXTRA_ARGS is split on whitespace on purpose.
	# shellcheck disable=SC2086
	set -- "$@" $EXTRA_ARGS
	printf '%s\n' "$@"
}

start() {
	if pid=$(running_pid); then
		echo "scouter-go already running (pid $pid)"
		return 0
	fi
	[ -x "$BIN" ] || { echo "binary not found or not executable: $BIN" >&2; exit 1; }
	mkdir -p "$DB" "$LOG_DIR" "$CONF_DIR"
	# Build the argument list once so it can be logged exactly as used.
	args=$(build_args)
	echo "$(date '+%Y/%m/%d %H:%M:%S') scouter-go.sh start: $BIN $(echo "$args" | tr '\n' ' ')" >>"$LOG_FILE"
	http=$(http_addr)
	if [ "$http" != "$HTTP" ]; then
		echo "$(date '+%Y/%m/%d %H:%M:%S') scouter-go.sh start: /health moved to $http (the Java HTTP servlets use port ${HTTP##*:}; set SCOUTER_HTTP to choose)" >>"$LOG_FILE"
	fi
	before=$(wc -l <"$LOG_FILE")
	(
		# Run from the script directory like the Java startup scripts: relative paths in
		# scouter.conf (geoip_data_city_file, ...) and the <pid>.scouter exit file that
		# the Java stop.sh removes resolve there, not in the caller's directory.
		cd "$HERE" || exit 1
		[ -n "$GOMAXPROCS_VALUE" ] && export GOMAXPROCS="$GOMAXPROCS_VALUE"
		[ -n "$GOMEMLIMIT_VALUE" ] && export GOMEMLIMIT="$GOMEMLIMIT_VALUE"
		# shellcheck disable=SC2046
		exec "$BIN" $(echo "$args" | tr '\n' ' ') >>"$LOG_FILE" 2>&1
	) </dev/null &
	pid=$!
	echo "$pid" >"$PID_FILE"
	# The server logs "Scouter Go listening" once the sockets are bound.
	i=0
	while [ $i -lt 50 ]; do
		if ! kill -0 "$pid" 2>/dev/null; then
			echo "scouter-go exited during start; last log lines:" >&2
			tail -n 20 "$LOG_FILE" >&2
			rm -f "$PID_FILE"
			exit 1
		fi
		if tail -n +"$((before + 1))" "$LOG_FILE" | grep -q "Scouter Go listening"; then
			break
		fi
		sleep 0.2
		i=$((i + 1))
	done
	echo "scouter-go started (pid $pid), log $LOG_FILE"
	tail -n 1 "$LOG_FILE"
}

stop() {
	if ! pid=$(running_pid); then
		echo "scouter-go not running"
		rm -f "$PID_FILE"
		return 0
	fi
	# SIGTERM: the server stops accepting, drains accepted work, flushes and
	# fsyncs, then logs the final counters and exits 0.
	kill -TERM "$pid"
	i=0
	while kill -0 "$pid" 2>/dev/null; do
		if [ $i -ge "$STOP_TIMEOUT" ]; then
			echo "scouter-go did not stop within ${STOP_TIMEOUT}s; sending SIGKILL (storage may need recovery on next start)" >&2
			kill -KILL "$pid" 2>/dev/null || true
			break
		fi
		sleep 1
		i=$((i + 1))
	done
	rm -f "$PID_FILE"
	echo "scouter-go stopped (pid $pid)"
}

status() {
	if pid=$(running_pid); then
		echo "scouter-go running (pid $pid)"
		ps -o pid=,etime=,rss=,args= -p "$pid" 2>/dev/null || true
		# The address this process was started with (the last --http wins).
		http=$(ps -o args= -p "$pid" 2>/dev/null | sed -n 's/.* --http[ =]\([^ ]*\).*/\1/p')
		if [ -n "$http" ] && command -v curl >/dev/null 2>&1; then
			curl -s --max-time 2 "http://$http/health" || true
			echo
		fi
		grep ' stats ' "$LOG_FILE" 2>/dev/null | tail -n 1
		return 0
	fi
	echo "scouter-go not running"
	return 3
}

case ${1:-} in
start) start ;;
stop) stop ;;
restart) stop; start ;;
status) status ;;
args) build_args ;;
*)
	echo "usage: $0 {start|stop|restart|status|args}" >&2
	exit 2
	;;
esac
